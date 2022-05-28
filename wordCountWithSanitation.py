from pyspark.sql import SparkSession
from argparse import ArgumentParser
from operator import add
import re

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--input", type=str, help="Input file txt", required=True)
    parser.add_argument("--output", type=str, help="Output directory")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("wor_count").getOrCreate())

    counts = (spark.read.text(args.input)
        .rdd
        .map(lambda r: r[0])
        .map(lambda x: re.sub(r"[^A-Za-z0-9' -]+","",x))
        .map(lambda x: x.lower())
        .flatMap(lambda x: x.split(" "))
        .filter(lambda x: len(x) > 0)
        .map(lambda x: (x,1))
        .reduceByKey(add)
        .sortBy(lambda x: x[1], False))

    output = spark.createDataFrame(counts).toDF("word","count")

    if not args.output:
        print(output.show())
    else:
        output.write.csv(args.output)