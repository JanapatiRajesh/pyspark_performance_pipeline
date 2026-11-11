import argparse, json, logging, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

TX_SCHEMA=StructType([StructField(c,StringType(),True) for c in ["transaction_id","customer_id","amount","currency","event_ts","status"]])
CUSTOMER_SCHEMA=StructType([StructField(c,StringType(),True) for c in ["customer_id","customer_name","city"]])
CURRENCIES=["INR","USD","EUR"]

def now(): return datetime.now(timezone.utc).isoformat()

def build_spark(master="local[*]", shuffle_partitions=8):
    return (SparkSession.builder.appName("YottaFlex-PySpark-Optimization").master(master)
            .config("spark.sql.shuffle.partitions",str(shuffle_partitions))
            .config("spark.sql.adaptive.enabled","true").getOrCreate())

def read_sources(spark, transactions_path, customers_path):
    tx=spark.read.option("header",True).schema(TX_SCHEMA).csv(transactions_path)
    dim=spark.read.option("header",True).schema(CUSTOMER_SCHEMA).csv(customers_path)
    return tx,dim

def validate_and_prepare(tx, dim):
    duplicate_dim=(dim.groupBy("customer_id").count().where("count > 1")
                   .select("customer_id").withColumn("duplicate_customer_key",F.lit(True)))
    dim_clean=dim.dropDuplicates(["customer_id"]).join(duplicate_dim,"customer_id","left")
    parsed=(tx.withColumn("amount_num",F.expr("try_cast(amount as decimal(18,2))"))
            .withColumn("event_timestamp",F.to_timestamp("event_ts")))
    win=Window.partitionBy("transaction_id").orderBy(F.col("event_ts").asc_nulls_last(),F.col("customer_id").asc_nulls_last())
    parsed=parsed.withColumn("duplicate_rank",F.row_number().over(win))
    joined=parsed.join(dim_clean,"customer_id","left")
    reason=(F.when(F.col("transaction_id").isNull()|(F.trim("transaction_id")==""),"missing_transaction_id")
      .when(F.col("duplicate_rank")>1,"duplicate_transaction_id")
      .when(F.col("customer_id").isNull()|(F.trim("customer_id")==""),"missing_customer_id")
      .when(F.col("amount_num").isNull(),"malformed_amount")
      .when(F.col("amount_num")<=0,"amount_must_be_positive")
      .when(~F.col("currency").isin(CURRENCIES),"unsupported_currency")
      .when(F.col("event_timestamp").isNull(),"invalid_event_timestamp")
      .when(F.col("duplicate_customer_key")==True,"duplicate_customer_dimension_key")
      .when(F.col("customer_name").isNull(),"unknown_customer_id"))
    checked=joined.withColumn("reject_reason",reason)
    rejects=(checked.where(F.col("reject_reason").isNotNull())
      .select("transaction_id","customer_id","amount","currency","event_ts","reject_reason")
      .withColumn("source_file",F.lit("transactions.csv")))
    accepted=(checked.where(F.col("reject_reason").isNull())
      .select("transaction_id","customer_id","customer_name","city",F.col("amount_num").alias("amount"),
              "currency",F.col("event_timestamp").alias("event_ts"),"status"))
    return accepted,rejects

def baseline_transform(accepted, dim):
    joined=accepted.drop("customer_name","city").join(dim.select("customer_id","customer_name","city"),"customer_id")
    return joined.groupBy("customer_id","customer_name","city","currency").agg(
      F.count("*").alias("transaction_count"),F.sum("amount").cast("decimal(20,2)").alias("total_amount"),
      F.max("event_ts").alias("latest_event_ts"))

def optimized_transform(accepted, dim, partitions=8):
    fact=accepted.repartition(partitions,"customer_id").drop("customer_name","city")
    joined=fact.join(F.broadcast(dim.select("customer_id","customer_name","city")),"customer_id")
    return joined.groupBy("customer_id","customer_name","city","currency").agg(
      F.count("*").alias("transaction_count"),F.sum("amount").cast("decimal(20,2)").alias("total_amount"),
      F.max("event_ts").alias("latest_event_ts"))

def equal_results(a,b):
    return a.exceptAll(b).limit(1).count()==0 and b.exceptAll(a).limit(1).count()==0

def run_pipeline(transactions="data/input/transactions.csv", customers="data/input/customers.csv",
                 output_dir="data/output", report_dir="reports", shuffle_partitions=8, master="local[*]"):
    run_id=str(uuid.uuid4()); started=now(); out=Path(output_dir); reports=Path(report_dir)
    out.mkdir(parents=True,exist_ok=True); reports.mkdir(parents=True,exist_ok=True)
    spark=build_spark(master,shuffle_partitions); spark.sparkContext.setLogLevel("WARN")
    try:
        tx,dim=read_sources(spark,transactions,customers); input_count=tx.count()
        accepted,rejects=validate_and_prepare(tx,dim); accepted=accepted.cache(); rejects=rejects.cache()
        accepted_count=accepted.count(); rejected_count=rejects.count()
        t=time.perf_counter(); base=baseline_transform(accepted,dim).cache(); base_count=base.count()
        base.write.mode("overwrite").parquet(str(out/"baseline")); baseline_s=time.perf_counter()-t
        t=time.perf_counter(); opt=optimized_transform(accepted,dim,shuffle_partitions).cache(); opt_count=opt.count()
        opt.write.mode("overwrite").partitionBy("city").parquet(str(out/"optimized")); optimized_s=time.perf_counter()-t
        equivalent=equal_results(base,opt)
        if not equivalent: raise AssertionError("Baseline and optimized results differ")
        accepted.withColumn("run_id",F.lit(run_id)).write.mode("overwrite").parquet(str(out/"accepted"))
        rejects.withColumn("run_id",F.lit(run_id)).write.mode("overwrite").option("header",True).csv(str(out/"rejected"))
        report={"run_id":run_id,"status":"SUCCESS","started_at":started,"ended_at":now(),
          "source_files":[transactions,customers],"input_rows":input_count,"accepted_rows":accepted_count,
          "rejected_rows":rejected_count,"baseline_output_groups":base_count,"optimized_output_groups":opt_count,
          "baseline_seconds":round(baseline_s,3),"optimized_seconds":round(optimized_s,3),
          "speedup_ratio":round(baseline_s/optimized_s,3) if optimized_s else None,"outputs_equivalent":equivalent,
          "reconciliation":{"input_equals_accepted_plus_rejected":input_count==accepted_count+rejected_count,
          "baseline_equals_optimized":equivalent,"output_group_counts_match":base_count==opt_count},
          "optimization":["repartition fact rows by customer_id","broadcast small customer dimension",
                          "write optimized output partitioned by city"]}
        (reports/"performance_report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
        print(json.dumps(report,indent=2)); base.unpersist(); opt.unpersist(); accepted.unpersist(); rejects.unpersist()
        return report
    except Exception as e:
        failure={"run_id":run_id,"status":"FAILED","started_at":started,"ended_at":now(),"error":repr(e)}
        (reports/"failure_report.json").write_text(json.dumps(failure,indent=2),encoding="utf-8")
        logging.exception("Pipeline failed, run_id=%s",run_id); raise
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--transactions",default="data/input/transactions.csv"); p.add_argument("--customers",default="data/input/customers.csv")
    p.add_argument("--output-dir",default="data/output"); p.add_argument("--report-dir",default="reports")
    p.add_argument("--shuffle-partitions",type=int,default=8); p.add_argument("--master",default="local[*]")
    a=p.parse_args(); run_pipeline(a.transactions,a.customers,a.output_dir,a.report_dir,a.shuffle_partitions,a.master)
