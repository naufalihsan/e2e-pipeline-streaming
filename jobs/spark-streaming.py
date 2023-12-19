import os
import time
from dotenv import load_dotenv
from transformers import pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, when, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

load_dotenv()

kafka_sasl_username = os.getenv("KAFKA_SASL_USERNAME")
kafka_sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
kafka_sasl_mechanisms = os.getenv("KAFKA_SASL_MECHANISMS")
kafka_session_timeout_ms = os.getenv("KAFKA_SESSION_TIMEOUT_MS")

schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
schema_registry_basic_auth_user_info = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")


def sentiment_analysis(review: str) -> str:
    predict = pipeline(
        "sentiment-analysis",
        model="siebert/sentiment-roberta-large-english",
    )

    return predict(review)[0]["label"]


def stream_to_kafka(session: SparkSession):
    while True:
        try:
            stream_df = (
                session.readStream.format("socket")
                .option("host", "0.0.0.0")
                .option("port", "9999")
                .load()
            )

            schema = StructType(
                [
                    StructField("review_id", StringType()),
                    StructField("user_id", StringType()),
                    StructField("business_id", StringType()),
                    StructField("stars", FloatType()),
                    StructField("date", StringType()),
                    StructField("text", StringType()),
                ]
            )

            stream_df = stream_df.select(
                from_json(col("value"), schema).alias("data")
            ).select(("data.*"))

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn(
                "feedback",
                when(
                    col("text").isNotNull(), sentiment_analysis_udf(col("text"))
                ).otherwise(None),
            )

            kafka_df = stream_df.selectExpr(
                "CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value"
            )

            _ = (
                kafka_df.writeStream.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("kafka.security.protocol", kafka_security_protocol)
                .option("kafka.sasl.mechanism", kafka_sasl_mechanisms)
                .option(
                    "kafka.sasl.jaas.config",
                    'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                    'password="{password}";'.format(
                        username=kafka_sasl_username,
                        password=kafka_sasl_password,
                    ),
                )
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("topic", "yelp_review_analysis")
                .start()
                .awaitTermination()
            )
        except Exception as e:
            print(f"Exception encountered: {e}. Retrying in 10 seconds")
            time.sleep(10)


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("socket_stream_to_kafka").getOrCreate()
    stream_to_kafka(spark_session)
