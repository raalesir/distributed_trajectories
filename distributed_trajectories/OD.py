"""
    Origin-Destination Matrix
    =========================
"""

from pyspark.sql.types import  ArrayType, FloatType
import  pyspark.sql.functions as F
from pyspark.sql import Window


try:
    # imports for pytest and documentation

    from distributed_trajectories.udfs  import  d1_state_vector, updates_to_the_transition_matrix
    from distributed_trajectories.consts import  width, lat_cells, lon_cells, OD_time_frame
except:
    # imports for running the package.

    from udfs import d1_state_vector, updates_to_the_transition_matrix
    from consts import width, lat_cells, lon_cells, OD_time_frame


class OD:
    """
    calculates Origin-Destination matrix
    for the given PySpark Data Frame
    """

    def __init__(self, df):
        """

        :param df: PySpark DataFrame
        """

        self.df =  df


    def set_d1_state_vector(self):
        """
        returns 1D representation for the distributed state
        :return:
        """

        d1_state_vector_udf = F.udf(d1_state_vector,
                                    ArrayType(ArrayType(FloatType()))
                                    )
        window = Window.partitionBy(['id', F.to_date('avg_ts')]).orderBy('ts').rangeBetween(-OD_time_frame, OD_time_frame)


        self.df = self.df \
            .withColumn('ts', F.col('avg_ts').cast('long')) \
            .withColumn('first_ts', F.first('avg_ts').over(window)) \
            .withColumn('last_ts', F.last('avg_ts').over(window)) \
            .withColumn('first_lat_idx', F.first('lat_idx').over(window)) \
            .withColumn('last_lat_idx', F.last('lat_idx').over(window)) \
            .withColumn('first_lon_idx', F.first('lon_idx').over(window)) \
            .withColumn('last_lon_idx', F.last('lon_idx').over(window)) \
            .withColumn('d1_states1',
                        d1_state_vector_udf(F.col('first_lon_idx'), F.col('first_lat_idx'), F.lit(width), F.lit(lon_cells), F.lit(lat_cells))
                        ) \
            .withColumn('d1_states2',
                        d1_state_vector_udf(F.col('last_lon_idx'), F.col('last_lat_idx'), F.lit(width), F.lit(lon_cells), F.lit(lat_cells))
                        )\


    def set_OD_updates(self):
        """
        for each  pair of (origin,  destination) create entry for the OD  matrix
        :return:
        """

        updates_to_OD_udf = F.udf(updates_to_the_transition_matrix,
                                  ArrayType(ArrayType(FloatType()))
                                  )

        self.df = self.df.withColumn('updates_to_OD', updates_to_OD_udf(F.col('d1_states1'), F.col('d1_states2')))


    def  collect_OD_updates(self):
        """
        sums up the  contributions for different Origin-Destination pairs
        :return:
        """


        OD = self.df.select('updates_to_OD')\
            .withColumn('updates_to_OD', F.explode('updates_to_OD'))\
            .withColumn("x", F.col('updates_to_OD').getItem(0))\
            .withColumn("y", F.col('updates_to_OD').getItem(1))\
            .withColumn("val", F.col('updates_to_OD').getItem(2))\
            .drop('updates_to_OD')\
            .groupBy(['x', 'y']).agg(F.sum('val').alias('updates_to_OD'))

        return OD


    def make_od(self):
        """
        includes all the steps for OD
        :return:
        """
        print('setting  1D state vector')
        self.set_d1_state_vector()
        print("setting  OD updates")
        self.set_OD_updates()
        print("aggregating OD updates")
        od = self.collect_OD_updates()

        return od

