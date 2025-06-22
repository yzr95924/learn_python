from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
import pyarrow.parquet as pq
import logging

g_test_parquet_path = "./tmp/test.parquet"
g_namespace_name = "yzr"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S"
)

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
    logging.info("connect to catalog done")

    df = pq.read_table(g_test_parquet_path)

    # first create namespace
    logging.info("create namespace")
    try:
        local_catalog.create_namespace(g_namespace_name)
    except NamespaceAlreadyExistsError as e:
        logging.warning("namespace already exists")

    logging.info("create table")
    try:
        table = local_catalog.create_table(
            f"{g_namespace_name}.test_table",
            schema=pq.read_schema(g_test_parquet_path)
        )
    except TableAlreadyExistsError as e:
        logging.warning("table already exists")