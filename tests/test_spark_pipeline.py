import pytest
from src.pipeline import build_spark, read_sources, validate_and_prepare, baseline_transform, optimized_transform, equal_results
from src.data_generator import generate_data

@pytest.fixture(scope="module")
def spark():
    s = build_spark("local[2]", 2)
    yield s
    s.stop()

def test_validation_rejects_and_reconciles(spark,tmp_path):
    c,t=generate_data(str(tmp_path),1100,10,42); tx,dim=read_sources(spark,t,c)
    good,bad=validate_and_prepare(tx,dim)
    assert good.count()+bad.count()==tx.count()
    assert bad.count()>0

def test_baseline_and_optimized_match(spark,tmp_path):
    c,t=generate_data(str(tmp_path),1100,10,42); tx,dim=read_sources(spark,t,c)
    good,_=validate_and_prepare(tx,dim)
    assert equal_results(baseline_transform(good,dim),optimized_transform(good,dim,2))

def test_empty_batch(spark,tmp_path):
    c,t=generate_data(str(tmp_path),5,2,3); tx,dim=read_sources(spark,t,c)
    good,bad=validate_and_prepare(tx.limit(0),dim)
    assert good.count()==0 and bad.count()==0
