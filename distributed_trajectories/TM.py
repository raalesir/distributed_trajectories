
"""
    Transition Matrix
    =================
"""

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, FloatType

try:
    # imports for pytest and documentation
    from distributed_trajectories.consts import width, lat_cells, lon_cells, delta_avg_ts
    from distributed_trajectories.udfs import d1_state_vector, updates_to_the_transition_matrix
except:
    # imports for running the package.
    from consts import width, lat_cells, lon_cells, delta_avg_ts
    from udfs import d1_state_vector, updates_to_the_transition_matrix


class TM:

    """
    creates Transition Matrix
    """

    def __init__(self, df):
        """
        takes PySpark DF to create TM

        :param df: PySpark  dataframe
        """

        self.df = df


    def set_d1_state_vector(self):
        """
        returns 1D representation for the distributed state

        :return:  Transition Matrix object
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


    def set_timestamp_delta(self):
        """
        adds a time delta column, the difference in time between successive observations

        :return: Transition Matrix object
        """
        window = Window.partitionBy([F.col('id'), F.to_date(F.col('avg_ts'))]).orderBy(F.col('avg_ts'))

        self.df = self.df.withColumn('delta_avg_ts', F.col('avg_ts').cast('long') - F.lag(F.col('avg_ts')).over(window).cast('long'))


    def set_TM_updates(self):
        """
        for each  pair of (origin,  destination) create entry for the OD  matrix

        :return: Transition Matrix object
        """

        updates_to_TM_udf = F.udf(updates_to_the_transition_matrix,
                                  ArrayType(ArrayType(FloatType()))
                                  )

        self.df = self.df.withColumn('updates_to_TM', updates_to_TM_udf(F.col('d1_states'), F.col('d1_states_lag')))



    def collect_TM_updates(self):
        """
        sums up the  contributions for different Origin-Destination pairs

        :return: PySpark DF
        """

        TM = self.df.select(['updates_to_TM', 'delta_avg_ts']) \
            .withColumn('updates_to_TM', F.explode('updates_to_TM')) \
            .withColumn("x", F.col('updates_to_TM').getItem(0)) \
            .withColumn("y", F.col('updates_to_TM').getItem(1)) \
            .withColumn("val", F.col('updates_to_TM').getItem(2)) \
            .filter(F.col('delta_avg_ts') < delta_avg_ts)\
        .drop('updates_to_TM') \
            .groupBy(['x', 'y']).agg(F.sum('val').alias('updates_to_TM'))

        return TM


    @staticmethod
    def normalize_tm(tm):
        """
        normalizing  TM, so the sum over each row is 1

        :return: Transition Matrix
        """

        window = Window.partitionBy(F.col('x'))  # .orderBy(F.col('y'))

        tm = tm \
            .withColumn('updates_to_TM', F.col('updates_to_TM') / F.sum(F.col('updates_to_TM')).over(window))
        # .withColumn('number_of_items', F.count(F.col('y')).over(window)) \
        # .filter(F.col('number_of_items') > 10)

        return tm


    def make_tm(self):
        """
        includes all the steps for OD

        :return: PySpark DF, transition matrix
        """
        print('setting  1D state vector')
        self.set_d1_state_vector()
        print("setting timestamp delta")
        self.set_timestamp_delta()
        print("setting  TM updates")
        self.set_TM_updates()
        print("aggregating TM updates")
        tm = self.collect_TM_updates()
        tm = self.normalize_tm(tm)

        return tm