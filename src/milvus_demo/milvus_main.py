from pymilvus import MilvusClient, FieldSchema, DataType, CollectionSchema

if __name__ == "__main__":
    milvus_client = MilvusClient(
        db_name="test_db",
        uri="./tmp/milvus_demo.db"
    )

    milvus_client.drop_collection(
        collection_name="demo_table"
    )

    table_field = [
        # 主键
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=64)
    ]

    table_schema = CollectionSchema(
        fields=table_field,
        description="Table with scalar fields"
    )

    milvus_client.create_collection(
        collection_name="demo_table",
        schema=table_schema
    )