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
            .appName("inverted_index")
            .getOrCreate()
    )

    df = spark.read.text(args.input)

    output = (
        df
            # Get file path
            .withColumnRenamed("value", "content")
            .withColumn("path", F.input_file_name())
            .withColumn("path", F.split(F.col("path"), "/"))
            .withColumn("path", F.element_at(F.col("path"), -1))

            # Convert content to words and count the frequency
            .withColumn(
                "content",
                F.explode(
                    F.split(F.col("content"), " ")
                )
            )
            .withColumnRenamed("content", "word")
            .groupBy(["word", "path"]).count()

            # Convert the structure
            .withColumn(
                "path_count",
                F.concat(
                    F.lit("("),
                    F.col("path"),
                    F.lit(", "),
                    F.col("count"),
                    F.lit(")")
                )
            )
            .select(F.col("word"), F.col("path_count"))
            .groupBy(F.col("word")).agg(F.collect_list("path_count"))
            .withColumnRenamed("collect_list(path_count)", "value_pair")
            .withColumn(
                "value_pair",
                F.concat(
                    F.lit("["),
                    F.concat_ws(", ", F.col("value_pair")),
                    F.lit("]")
                )
            )
    )

    if not args.output:
        print(output.show())
    else:
        output.write.csv(args.output)