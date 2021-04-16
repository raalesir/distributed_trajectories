
"""
Tests for `distributed_trajectories` package
"""

import pytest

import  pyspark.sql.functions as F
from pyspark.sql.types import  TimestampType
from pyspark.sql import  Window

from distributed_trajectories.consts import spark

from distributed_trajectories.TM import TM
from distributed_trajectories.distributed_trajectories import PrepareDataset

class  TestTM:

    def test_normalize_tm(self):
        """
        testing how normalization works

        :return: true/false
        """

        df = spark.createDataFrame([(1, 2, 3),  (1,3,3),  (2,4,1), (2,3,3)], ['x',  'y', 'updates_to_TM'])
        res = TM.normalize_tm(df)
        tst = spark.createDataFrame([(1, 2, 0.5),  (1,3,.5),  (2,4,.25), (2,3,.75)], ['x',  'y', 'updates_to_TM'])

        assert res.collect() == tst.collect()



    def test_mocking_class_method(self, mocker):
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
        actual = [v[0] for  v  in actual]
        assert expected == actual





