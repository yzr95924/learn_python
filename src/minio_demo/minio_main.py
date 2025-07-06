from minio import Minio
import io

if __name__ == "__main__":
    minio_client = Minio(
        "192.168.100.210:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    text_data = "this is text data"
    text_buffer = io.BytesIO(text_data.encode("utf-8"))
    text_buffer_len = len(text_data.encode("utf-8"))

    bucket_list = minio_client.list_buckets()
    for bucket in bucket_list:
        print(f"find a bucket: {bucket}")
        minio_client.put_object(
            str(bucket),
            "test_data",
            text_buffer,
            text_buffer_len,
            content_type="text/plain"
        )