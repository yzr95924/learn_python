from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
import pyarrow.parquet as pq

g_test_parquet_path = "./tmp/test.parquet"

def connect_to_catalog() -> Catalog:
    warehouse_path = "./tmp"
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    return catalog

if __name__ == "__main__":
    local_catalog = connect_to_catalog()
    print("connect to catalog done")

    df = pq.read_table(g_test_parquet_path)

    # first create namespace
    local_catalog.create_namespace("default")
    table = local_catalog.create_table(
        "default.test_table",
        schema=pq.read_schema(g_test_parquet_path)
    )
    print("create table done")
