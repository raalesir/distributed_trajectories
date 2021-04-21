"""
    =============================================================
    Tests for `distributed_trajectories.distributed_trajectories`
    =============================================================
"""


import  pyspark.sql.functions as F
from distributed_trajectories.distributed_trajectories import PrepareDataset
from pyspark.sql import  Window
from pyspark.sql.types import  TimestampType

from distributed_trajectories.consts import spark



class  TestsBase:
    """
    Testing
    """

    def test_set_helper_column(self, mocker):
        """
        checking how `helper` column is being created.
        The `helper` column has the following meaning. Its value increases  by 1 as soon as
        an object leaves the 2D cell.

        :param mocker:
        :return:  false/True
        """
        expected = [1, 2, 2, 2, 3, 1, 2]

        def mock_read(self):
            tst = spark.createDataFrame(
                [(2, '2008-02-04 00:14:04', 116.385, 39.98, 10, 10),
                 (2, '2008-02-04 00:19:04', 116.355, 40.02, 9, 11),
                 (2, '2008-02-04 00:24:04', 116.325, 40.06, 8, 12),
                 (1, '2008-02-04 01:04:04', 116.325, 40.02, 8, 11),
                 (1, '2008-02-04 01:09:04', 116.325, 39.98, 8, 10),
                 (1, '2008-02-04 01:14:04', 116.355, 39.98, 9, 10),
                 (1, '2008-02-04 01:19:04', 116.355, 39.98, 9, 10),
                 (1, '2008-02-04 01:24:04', 116.355, 39.98, 9, 10),
                 (1, '2008-02-04 01:34:04', 116.415, 39.98, 11, 10)
                 ], ['id', 'ts', 'lon_middle', 'lat_middle', 'lon_idx', 'lat_idx']
            ) \
                .withColumn('ts', F.col('ts').cast(TimestampType()))

            return tst

        mocker.patch(
            'distributed_trajectories.distributed_trajectories.PrepareDataset.read',
            mock_read
        )
        actual = PrepareDataset('').set_helper_column().select('helper').collect()
        actual = [v[0] for v in actual]
        assert expected == actual


    def test_helper_column_set_avg_time_for_cell(self, mocker):
        """
        the  sequence of values in the `helper` column must be a increasing by 1 sequence for each `ID` and each `date`.

        :param mocker:
        :return: true/false
        """

        def mock_read(self):
            tst = spark.createDataFrame(
                [
                    (2, '2008-02-04 00:19:04', 116.355, 40.02, 9, 11, 1),
                    (2, '2008-02-04 00:24:04', 116.325, 40.06, 8, 12, 2),
                    (1, '2008-02-04 01:09:04', 116.325, 39.98, 8, 10, 1),
                    (1, '2008-02-04 01:14:04', 116.355, 39.98, 9, 10, 2),
                    (1, '2008-02-04 01:19:04', 116.355, 39.98, 9, 10, 2),
                    (1, '2008-02-04 01:24:04', 116.355, 39.98, 9, 10, 2),
                    (1, '2008-02-04 01:34:04', 116.415, 39.98, 11, 10, 3)
                ], ['id', 'ts', 'lon_middle', 'lat_middle', 'lon_idx', 'lat_idx', 'helper']
            ) \
                .withColumn('ts', F.col('ts').cast(TimestampType()))

            return tst

        mocker.patch(
            'distributed_trajectories.distributed_trajectories.PrepareDataset.read',
            mock_read
        )
        actual = PrepareDataset('').set_avg_time_for_cell()

        window = Window.partitionBy([F.col('id'), F.to_date(F.col('avg_ts'))]).orderBy(F.col('avg_ts'))

        res = actual.withColumn('check_helper', F.lag(F.col('helper')).over(window) - F.col('helper')) \
            .select(F.col('check_helper')) \
            .filter(F.col('check_helper') != -1) \
            .count()

        assert res == 0