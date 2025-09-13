import pyarrow as pa
from app.tasks.silver import transform_silver

def test_transform_contract_exists():
    # We don't execute against MinIO here; just assert function exists and is callable.
    assert callable(transform_silver.fn)
    tbl = pa.table({
        "id": ["1","1","2"],
        "name": ["A","A","B"],
        "brewery_type": ["micro","micro","brewpub"],
        "country": ["US","US","US"],
        "state": ["CA","CA","NY"],
        "city": ["X","X","Y"],
        "postal_code": ["1","1","2"],
        "latitude": [None, None, 10.0],
        "longitude": [None, None, -20.0],
    })
    assert set(tbl.column_names) == {
        "id","name","brewery_type","country","state","city","postal_code","latitude","longitude"
    }
