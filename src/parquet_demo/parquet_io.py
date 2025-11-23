import pyarrow.parquet as pq
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S"
)

G_NEIGHBORS_PARQUET_FILE_PATH = (
    "/tmp/vectordb_bench/dataset/cohere/cohere_medium_1m/neighbors.parquet"
)

if __name__ == "__main__":
    logging.info("Reading Parquet file from %s", G_NEIGHBORS_PARQUET_FILE_PATH)
    table = pq.read_table(G_NEIGHBORS_PARQUET_FILE_PATH,
                          columns=["neighbors_id"])
    logging.info("Parquet file read successfully")
    logging.info("Table schema: %s", table.schema)
    logging.info("Number of rows: %d", table.num_rows)
    metadata = pq.read_metadata(G_NEIGHBORS_PARQUET_FILE_PATH)
    df = table.to_pandas()
    logging.info("First 5 rows of the DataFrame:\n%s", df.head())