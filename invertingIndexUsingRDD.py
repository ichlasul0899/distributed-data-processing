from pyspark.sql import SparkSession
from argparse import ArgumentParser
from operator import add


def get_filename(path):
    return path.split("/")[-1]


def create_pairs(t):
    document = t[0]
    records = t[1]
    pairs = []
    for rec in records:
        for word in rec.split(" "):
            pairs.append((word, document))
    return pairs


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

    rdd = spark.sparkContext.wholeTextFiles(args.input)

    output = (
        rdd
            # Map to -> filename, content
            .map(lambda x: (get_filename(x[0]), x[1]))

            # Split to lines
            .map(lambda x: (x[0], x[1].splitlines()))

            # Create invert mapping, assign initial counter, reduce to get
            # the frequency
            .flatMap(create_pairs)
            .map(lambda x: (x, 1))
            .reduceByKey(add)

            # Convert the structure from ((word, path), frequency) to (word, (path, frequency))
            .map(lambda x: (x[0][0], (x[0][1], x[1])))

            # group the key (word) and collect the value into list of (path, frequency)
            .groupByKey()
            .mapValues(lambda v: list(v))
    )

    if not args.output:
        print(output.take(5))
    else:
        output.saveAsTextFile(args.output)