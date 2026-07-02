from vitals import streaming


def test_kafka_connector_package_matches_spark_scala_version():
    # Spark 4+ ships Scala 2.13; Spark 3.x ships 2.12. Coordinate version == pyspark version.
    assert streaming.kafka_connector_package("4.1.2") == \
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
    assert streaming.kafka_connector_package("3.5.1") == \
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def test_module_exposes_shared_transform_symbols():
    # The `from vitals import streaming` at the top of this file already proves the module imports
    # without pyspark/kafka (they're function-local); assert the shared symbols exist.
    assert hasattr(streaming, "clean_wearables")
    assert hasattr(streaming, "SCHEMA")
    assert hasattr(streaming, "kafka_connector_package")
