"""Main module."""

from  pyspark.sql import  SparkSession

import pyspark.sql.functions as F

from pyspark.sql.types import StructType, StructField,\
    StringType, FloatType, IntegerType, TimestampType,\
    ArrayType

from  pyspark.sql import  Window

from  consts  import  beijing_lat_box,  beijing_lon_box, lat_cells, lon_cells

from OD import  OD
from TM import TM

from udfs import   middle_interval_for_x




INPUT = '../data/*'



class PrepareDataset:
    """
    reading, filtering and preparing Dataset
    """

    # beijing_lat_box = [39.6, 40.2]
    # beijing_lon_box = [116.1, 116.7]


    def __init__(self, path):
        """

        :param path: dataset location
        """

        self.path = path

        self.schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('ts', TimestampType(),  True),
            StructField('lon', FloatType(), True),
            StructField('lat', FloatType(), True)
        ])

        self.df = self.read()


    def read(self):
        """
        reading  Pyspark DF

        :return: PySpark DF
        """

        df = spark.read.csv(self.path,\
                       schema=self.schema,\
                       mode="DROPMALFORMED")

        return df


    def get_data(self):
        """
        getter

        :return: `self.df`
        """

        self._process_data()

        return self.df

    def _process_data(self):
        """
        helper method  for data processing

        :return: Nothing
        """

        print('cropping data')
        self.crop_data()
        print("setting mean for lat,lon")
        self.set_middle_interval_for_x()
        print("set helper column")
        self.set_helper_column()
        print("calculate avg time for  cell")
        self.set_avg_time_for_cell()


    def set_avg_time_for_cell(self):
        """
        for each  cell we  calculate time  as  the average time  for all points  in that cell
        :return: `self.df`
        """

        self.df = self.df.groupBy(['id', F.to_date(F.col('ts')).alias('date'), 'lat_idx', 'lon_idx', 'helper']) \
            .agg(F.avg('ts').cast('timestamp').alias('avg_ts')) \
            .drop('date')



    def crop_data(self):
        """
        crops dataset  to fit into Beijing Box

        :return:  filtered `self.df`
        """

        self.df = self.df\
            .filter(F.col('lat') >= beijing_lat_box[0])\
            .filter(F.col('lat') <= beijing_lat_box[1])\
            .filter(F.col('lon') >= beijing_lon_box[0])\
            .filter(F.col('lon') <= beijing_lon_box[1]) \
            .drop_duplicates()



    def  get_schema(self):
        """
        DF schema getter
        :return:
        """
        return self.df.printSchema()


    def set_middle_interval_for_x(self):
        """
        for each `x`  returns the  value for  `x` as the center of the cell, the `x` is falling into
        :return:
        """
        middle_interval_for_x_udf = F.udf(middle_interval_for_x, ArrayType(FloatType()))

        self.df = self.df.withColumn('lon_middle', middle_interval_for_x_udf(
            F.col('lon'),
            F.lit(beijing_lon_box[0]),
            F.lit(beijing_lon_box[1]),
            F.lit(lon_cells))
                                  )

        self.df =  self.df.withColumn('lat_middle', middle_interval_for_x_udf(
            F.col('lat'),
            F.lit(beijing_lat_box[0]),
            F.lit(beijing_lat_box[1]),
            F.lit(lat_cells))
                                )


        self.df = self.df.withColumn('lon_idx', F.col('lon_middle')[1].cast(IntegerType())) \
            .withColumn('lat_idx', F.col('lat_middle')[1].cast(IntegerType())) \
            .withColumn('lon_middle', F.col('lon_middle')[0]) \
            .withColumn('lat_middle', F.col('lat_middle')[0])



    def set_helper_column(self):
        """
        the helper column is needed for further aggregations
        :return: `self.df`
        """
        window = Window.partitionBy([F.col('id'), F.to_date(F.col('ts'))]).orderBy(F.col('ts'))

        self.df = self.df.withColumn('lon_idx_lag', F.lag(F.col('lon_idx')).over(window)) \
            .withColumn('lat_idx_lag', F.lag(F.col('lat_idx')).over(window)) \
            .withColumn('n1', F.col('lon_idx_lag') != F.col('lon_idx')) \
            .withColumn('n2', F.col('lat_idx_lag') != F.col('lat_idx')) \
            .withColumn('helper', F.sum((F.col('n1') | F.col('n2')).cast(IntegerType())).over(window)) \
            .select(['id', 'ts', 'lon_middle', 'lat_middle', 'lon_idx', 'lat_idx', 'helper']) \
            .dropna()






if __name__ == "__main__":
    spark = SparkSession.builder\
    .enableHiveSupport()\
    .appName('Distributed Trajectories') \
    .master("local[*]")\
    .getOrCreate()


    data  = PrepareDataset(INPUT)


    preprocessed = data.get_data()

    print(preprocessed.head())

    od = OD(preprocessed).make_od()

    print(od.head())

    tm = TM(preprocessed).make_tm()

    print(tm.head())
