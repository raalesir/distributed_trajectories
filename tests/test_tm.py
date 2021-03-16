"""
Tests for `distributed_trajectories` package
"""

import pytest
from distributed_trajectories.consts import spark

from distributed_trajectories.distributed_trajectories import  TM


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