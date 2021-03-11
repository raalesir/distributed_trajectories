import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, FloatType

from .udfs import d1_state_vector, updates_to_the_transition_matrix
from .consts import width, lat_cells, lon_cells

class TM:
    """
    creates Transition Matrix
    """

    def __init__(self, df):
        """
        takes PySpark DF to create TM
        :param df:
        """

        self.df = df

    def set_d1_state_vector(self):
        """
        returns 1D representation for the distributed state
        :return:
        """

        d1_state_vector_udf = F.udf(d1_state_vector,
                                    ArrayType(ArrayType(FloatType()))
                                    )

        window = Window.partitionBy([F.col('id'), F.to_date(F.col('avg_ts'))]).orderBy(F.col('avg_ts'))

        self.df = self.df \
            .withColumn('d1_states', d1_state_vector_udf(F.col('lon_idx'), F.col('lat_idx'), F.lit(width), F.lit(lon_cells), F.lit(lat_cells))
                        ) \
            .withColumn('d1_states_lag', F.lag(F.col('d1_states')).over(window)) \
            .dropna()


    def set_TM_updates(self):
        """
        for each  pair of (origin,  destination) create entry for the OD  matrix
        :return:
        """

        updates_to_TM_udf = F.udf(updates_to_the_transition_matrix,
                                  ArrayType(ArrayType(FloatType()))
                                  )

        self.df = self.df.withColumn('updates_to_TM', updates_to_TM_udf(F.col('d1_states'), F.col('d1_states_lag')))



    def collect_TM_updates(self):
        """
        sums up the  contributions for different Origin-Destination pairs
        :return:
        """

        TM = self.df.select('updates_to_TM') \
            .withColumn('updates_to_TM', F.explode('updates_to_TM')) \
            .withColumn("x", F.col('updates_to_TM').getItem(0)) \
            .withColumn("y", F.col('updates_to_TM').getItem(1)) \
            .withColumn("val", F.col('updates_to_TM').getItem(2)) \
            .drop('updates_to_TM') \
            .groupBy(['x', 'y']).agg(F.sum('val').alias('updates_to_TM'))

        return TM


    def make_tm(self):
        """
        includes all the steps for OD
        :return:
        """
        print('setting  1D state vector')
        self.set_d1_state_vector()
        print("setting  TM updates")
        self.set_TM_updates()
        print("aggregating TM updates")
        tm = self.collect_TM_updates()

        return tm