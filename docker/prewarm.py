"""Build-time warm-up: downloads the Delta jars into the image so the first
pipeline run is fast and works without network access."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

b = (SparkSession.builder.master("local[1]")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
s = configure_spark_with_delta_pip(b).getOrCreate()
s.range(1).write.format("delta").mode("overwrite").save("/tmp/_warm")
print("Delta jars cached; sanity rows =", s.read.format("delta").load("/tmp/_warm").count())
s.stop()
