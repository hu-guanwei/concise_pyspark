from functools import reduce
import pyspark.sql.functions as F


# flatten nested 2d list
flatten = lambda nested_lst: reduce(lambda head, tail: head + tail, nested_lst)

# get list of `Column` literal values from an iterable
lit_itr = lambda itr: [F.lit(x) for x in itr]

# compose a list of function into one
compose = lambda pipeline: lambda initial: reduce(lambda x, f: f(x), pipeline, initial)


def map_cols(dict_, input_col):
    """
    :type: dict_: Python dict object
    :type: input_col: str
    :rtype: composition of pySpark sql functions

    use this function to map values in a column to another

    >>> df = spark.createDataFrame([['a'], ['b'], ['c']])
    >>> d = {'a': 1, 'b': 2, 'c': 3}
    >>> df.withColumn('_2', map_cols(d, '_1')).show()
    +---+---+
    | _1| _2|
    +---+---+
    |  a|  1|
    |  b|  2|
    |  c|  3|
    +---+---+
    """
   
    pip = [
        flatten,
        lit_itr,
        F.create_map
    ]
   
    map_literals = compose(pip)(dict_.items())
    return map_literals.getItem(F.col(input_col))


def is_in(itr, input_col):
    """
    :param: itr: iterable
    :param: input_col: str

    check if the values in input_col is in itr
    """
    return F.col(input_col).isin(lit_itr(itr))
