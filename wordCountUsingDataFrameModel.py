from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from argparse import ArgumentParser


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--input", type=str, help="Input file txt", required=True)
    parser.add_argument("--output", type=str, help="Output directory")
    args = parser.parse_args()

    spark = (
        SparkSession
            .builder
            .appName("word_count")
            .getOrCreate()
    )

    df = spark.read.text(args.input)

    output = (
        df
            .withColumnRenamed("value", "words")
            .withColumn(
                "split",
                F.split(F.col("words"), " ")
            )
            .select(
                F.explode(F.col("split")).alias("word")
            )
            .filter(F.length(F.col("word")) > 0)
            .withColumn(
                "word",
                F.regexp_replace(
                    F.col("word"),
                    r"[^A-Za-z0-9' -]+",
                    ""
                )
            )
            .withColumn("word", F.lower(F.col("word")))
            .groupBy("word").count()
            .orderBy(F.col("count").desc())
    )
    
    if not args.output:
        print(output.show())
    else:
        output.write.csv(args.output)