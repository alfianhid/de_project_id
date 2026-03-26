from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.jars",
                "/opt/bitnami/spark/jars/spark-bigquery-connector.jar,"
                "/opt/bitnami/spark/jars/gcs-connector.jar")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.bigquery.parentProject", "your-gcp-project-id")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/secrets/gcp-sa-key.json")
        .getOrCreate()
    )