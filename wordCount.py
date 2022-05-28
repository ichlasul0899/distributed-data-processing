from pyspark.sql import SparkSession
from argparse import ArgumentParser
from operator import add

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--input", type=str, help="Input file txt", required=True)
    parser.add_argument("--output", type=str, help="Output directory")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("wor_count").getOrCreate())

    counts = (spark.read.text(args.input).rdd.map(lambda r: r[0]).flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(add).sortBy(lambda x: x[1], False))

    output = spark.createDataFrame(counts).toDF("word","count")

    if not args.output:
        print(output.show())
    else:
        output.write.csv(args.output)