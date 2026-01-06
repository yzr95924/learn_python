"""pyspark demo"""
import logging
import time

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S"
)

def generate_data(partition_id):
    """_summary_

    Args:
        partition_id (_type_): _description_

    Returns:
        _type_: _description_
    """
    return [(partition_id, i, f"data_{i}") for i in range(1000)]

if __name__ == "__main__":
    spark_session = SparkSession.builder \
        .appName("TestApp") \
        .master("spark://cluster-ubuntu24-201:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    cse_df = spark_session.read.parquet(
        "hdfs://cluster-ubuntu24-201:8020/zryang/CSE-CIC-IDS2018.parquet"
    )
    logging.info("init partition num: %s", cse_df.rdd.getNumPartitions())
    start_time = time.perf_counter_ns()
    cse_df.createOrReplaceTempView("cse_view")
    result = spark_session.sql("""
                               SELECT Label FROM cse_view
                               WHERE Label=1
                               LIMIT 10
                               """)
    result.show()
    result.explain()
    logging.info("end %d", time.perf_counter_ns() - start_time)
    spark_session.stop()
