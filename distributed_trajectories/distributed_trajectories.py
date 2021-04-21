"""
    Main module
    ===========
"""


import numpy as np

import os

import matplotlib.pyplot as plt
import scipy.sparse as sparse


import pyspark.sql.functions as F

from pyspark.sql.types import StructType, StructField,\
    StringType, FloatType, IntegerType, TimestampType,\
    ArrayType

from  pyspark.sql import  Window


try:
        # imports for pytest and documentation
    from distributed_trajectories.consts import beijing_lat_box, beijing_lon_box, lat_cells, lon_cells, width, spark, OD_time_frame, speed, \
        timestamps_per_hour
    from distributed_trajectories.OD import OD
    from distributed_trajectories.TM import TM
    from distributed_trajectories.udfs import middle_interval_for_x, d1_state_vector, updates_to_the_transition_matrix
except ModuleNotFoundError:
    # imports for running the package.
    from consts import beijing_lat_box, beijing_lon_box, lat_cells, lon_cells, width, spark, OD_time_frame, speed,\
        timestamps_per_hour
    from OD import OD
    from TM import TM
    from udfs import middle_interval_for_x, d1_state_vector, updates_to_the_transition_matrix

INPUT = '../data/*'
PLOT_DIR = 'plots'



class PrepareDataset:
    """
    reading, filtering and preparing Dataset
    """

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
        print("keeping just those ID, which have more than %i point per hour" %(timestamps_per_hour))
        self.filter_too_sparse_IDs()
        print("removing duplicates for ID and timestamp")
        self.remove_duplicates()
        print("removing too fast objects")
        self.remove_too_fast_objects()

        print("setting mean for lat,lon")
        self.set_middle_interval_for_x()
        print("set helper column")
        self.set_helper_column()
        print("calculate avg time for  cell")
        self.set_avg_time_for_cell()


    def filter_too_sparse_IDs(self):
        """
        keeping just those tracks which have > `timestamps_per_hour` points per hour on average

        :return: filtered `self.df`
        """

        ids = self.df.groupBy(['id', F.to_date(F.col('ts'))]) \
            .count() \
            .filter(F.col('count') > timestamps_per_hour * 24) \
            .select('id') \
            .distinct()

        self.df = self.df.join(F.broadcast(ids), ['id'], how='inner')


    def remove_too_fast_objects(self):
        """
        some data entries are surely erroneus, so some objects move up to 10-20 km per second.
        We should remove it.

        :return:  filtered `self.df`
        """

        window = Window.partitionBy(['id', F.to_date('ts')]).orderBy('ts')

        self.df = self.df \
            .withColumn('delta_lat', (F.lag('lat').over(window) - F.col('lat'))) \
            .withColumn('delta_lon', (F.lag('lon').over(window) - F.col('lon'))) \
            .withColumn('delta_ts', (F.col('ts').cast('long') - F.lag('ts').over(window).cast('long'))) \
            .withColumn('speed1', F.col('delta_lat') / F.col('delta_ts')) \
            .withColumn('speed2', F.col('delta_lon') / F.col('delta_ts')) \
            .dropna() \
            .filter((F.abs(F.col('speed1')) < speed) & (F.abs(F.col('speed2')) < speed))


    def remove_duplicates(self):
        """
        removes duplicates for `ID` and `timestamp`

        :return: filtered `self.df`
        """
        self.df = self.df.dropDuplicates(['id', 'ts'])


    def set_avg_time_for_cell(self):
        """
        for each  cell we  calculate time  as  the average time  for all points  in that cell, `avg_ts`;
        The `ts_1` and `ts_2` are the timestamps for the user enter and leave the cell

        :return: `self.df`
        """

        self.df = self.df.groupBy(['id', F.to_date(F.col('ts')).alias('date'), 'lat_idx', 'lon_idx', 'helper']) \
            .agg(F.avg('ts').cast('timestamp').alias('avg_ts'),
                 F.first(F.col('ts')).alias('ts_1'),
                 F.last(F.col('ts')).alias("ts_2")
                 )\
            .drop('date')

        return  self.df



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

        return self.df




def prepare_for_plot(data, type_):
    """
    Takes PySpark DF and produces a sparse Scipy matrix to plot

    :param type_: Transition Matrix or Origin-Destination
    :param data: Pyspark DF
    :return: Sparse matrix  to  plot
    """

    pd_df = data.toPandas()

    data = np.array( pd_df[type_])
    rows = np.array( pd_df['y'].astype('int'))
    cols = np.array( pd_df['x'].astype('int'))

    A = sparse.coo_matrix((data, (rows, cols)))

    return  A


def plot(matrix, fname, title):
    """
    plotting sparse matrix

    :param title: title of the plot
    :param fname: output file name
    :param matrix: sparse Scipy matrix
    :return: Plot in `plots` folder
    """

    plt.figure(figsize=(20, 20))
    plt.spy(matrix, markersize=1, alpha=0.2)
    plt.grid()
    plt.xlabel("state", fontsize=16)
    plt.ylabel("state", fontsize=16)
    plt.title(title, fontsize=20)
    plt.savefig(os.path.join(PLOT_DIR, fname))



if __name__ == "__main__":


    spark = spark

    data  = PrepareDataset(INPUT)


    preprocessed = data.get_data()

    print(preprocessed.head())

    od = OD(preprocessed).make_od()
    plot(prepare_for_plot(od, 'updates_to_OD'), 'OD.png', "Origin-Destination matrix for n=%s, m=%s, width=%s"
         %(lat_cells, lon_cells, width))

    # pdf_od = od.toPandas()

    print(od.head())

    tm = TM(preprocessed).make_tm()

    plot(prepare_for_plot(tm,'updates_to_TM'), 'TM.png',"TM matrix for n=%s, m=%s, width=%s"  %(lat_cells, lon_cells, width))

    # print(tm.head())
