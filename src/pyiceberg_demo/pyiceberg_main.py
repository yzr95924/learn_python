"""pyiceberg demo main file"""
import logging
import os
from datetime import datetime

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table as IcebergTable
from pyiceberg.types import (
    TimestampType,
    DoubleType,
    StringType,
    NestedField,
    StructType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
import pyarrow as pa

G_TEST_PARQUET_PATH = "./tmp/test.parquet"
G_NAMESPACE_NAME = "yzr"
G_TABLE_NAME = "my_test_table"
G_TABLE_IDENTIFIER = G_NAMESPACE_NAME + "." + G_TABLE_NAME
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S"
)

def connect_to_sql_catalog() -> Catalog:
    """connect to Iceberg SQL catalog

    Returns:
        Catalog: object of SQL catalog
    """
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

def connect_to_local_rest_catalog() -> Catalog:
    """connect to Iceberg local rest catalog

    Returns:
        Catalog: object of local rest catalog
    """
    catalog = load_catalog("local_rest_catalog")
    return catalog

def create_test_table(catalog: Catalog) -> IcebergTable:
    """create test table in the given catalog

    Args:
        Catalog (Catalog): catalog object
    """
    # define table schema
    table_schema = Schema(
        NestedField(field_id=1, name="id", type=StringType(), required=False),
        NestedField(field_id=2, name="datetime", type=TimestampType(), required=False),
        NestedField(field_id=3, name="double_field", type=DoubleType(), required=False),
        NestedField(
            field_id=4,
            name="struct_field",
            field_type=StructType(
                NestedField(field_id=5, name="struct_str", type=StringType(), required=False),
                NestedField(field_id=6, name="struct_double", type=DoubleType(), required=False),
            ),
            required=False
        )
    )

    # define partition spec
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform="day", name="datetime_day")
    )

    # define sort order
    sort_order = SortOrder(
        SortField(source_id=1, transform="identity")
    )

    # use catalog to create table
    test_table = catalog.create_table(
        identifier=G_TABLE_IDENTIFIER,
        schema=table_schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        location=os.path.join("/tmp", G_TABLE_NAME)
    )
    logging.info("create table success")
    return test_table

if __name__ == "__main__":
    g_local_catalog = connect_to_local_rest_catalog()
    logging.info("connect to catalog done")

    # df = pq.read_table(g_test_parquet_path)

    # first create namespace
    logging.info("start to create namespace")
    try:
        g_local_catalog.create_namespace(G_NAMESPACE_NAME)
    except NamespaceAlreadyExistsError:
        logging.warning("namespace already exists")

    logging.info("start to list namespace")
    namespaces = g_local_catalog.list_namespaces()
    logging.info("namespaces: %s", namespaces)

    # then create table
    logging.info("start to create table")
    try:
        g_test_table = create_test_table(catalog=g_local_catalog)
    except TableAlreadyExistsError:
        logging.warning("table already exists")
        g_test_table = g_local_catalog.load_table(G_TABLE_IDENTIFIER)

    g_df = pa.Table.from_pylist([
        {
            "id": "row_001",
            "datetime": datetime(2023, 10, 26, 14, 30, 45),  # 必须为 datetime 对象
            "double_field": 12.34,
            "struct_field": {
                "struct_str": "inside_struct_1",
                "struct_double": 99.99
            }
        },
        {
            "id": "row_002"
            # 省略了 datetime, float_field, struct_field
        }
    ])
    logging.info(g_df.schema)

    g_test_table.append(df=g_df)
    g_test_table.update_schema().add_column(
        path="new_col", field_type=StringType(), required=False
    ).commit()

    print(g_test_table.last_sequence_number)
    # g_local_catalog.purge_table(G_TABLE_IDENTIFIER)
    # g_local_catalog.drop_namespace(G_NAMESPACE_NAME)
