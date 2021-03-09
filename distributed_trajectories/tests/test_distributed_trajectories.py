#!/usr/bin/env python

"""Tests for `distributed_trajectories` package."""

import pytest

from distributed_trajectories import udfs


class TestBasic:
    # INPUT = "data/input/dataset.jsonl"




    def test_d1_state_vector_1(self):
        """
        testing the transformation to 1D coordinates
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=5, width=1, m=7)
        res = [el[0] for el in res]

        assert res == [4.0, 5.0, 6.0, 11.0, 12.0, 13.0, 18.0, 19.0, 20.0]


    def test_d1_state_vector_2(self):
        """
        testing the transformation to 1D coordinates with `width==0`
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=5, width=0, m=7)
        res = [el[0] for el in res]

        assert res == [12.0]



    def test_d1_state_vector_3(self):
        """
        testing the transformation to 1D coordinates out of grid
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=7, width=1, m=7)
        res = [el[0] for el in res]

        assert res == []


