#!/usr/bin/env python

"""Tests for `distributed_trajectories` package."""

import pytest

from distributed_trajectories import udfs


class TestBasic:




    def test_d1_state_vector_1(self):
        """
        testing the transformation to 1D coordinates
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=4, width=1, m=7, n=5)
        res = [el[0] for el in res]

        assert res == [3.0, 4.0, 5.0, 8.0, 9.0, 10.0, 13.0, 14.0, 15.0]


    def test_d1_state_vector_2(self):
        """
        testing the transformation to 1D coordinates with `width==0`
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=5, width=0, m=7, n=7)
        res = [el[0] for el in res]

        assert res == [12.0]



    def test_d1_state_vector_3(self):
        """
        testing the transformation to 1D coordinates out of grid
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=17, width=1, m=7, n=17)
        res = [el[0] for el in res]

        assert res == []



    def test_middle_interval_x_1(self):
        """
        testing the coordinates for the middle of the interval and the box number
        middle_interval_for_x(x, A, B, m)
        :return:
        """
        res = udfs.middle_interval_for_x(4.67, 3, 7, 4)

        assert  res == (4.5, 2.0)



    def test_middle_interval_x_border_case1(self):
        """
        testing the coordinates for the middle of the interval and the box number
        middle_interval_for_x(x, A, B, m)
        :return:
        """
        res = udfs.middle_interval_for_x(3, 3, 7, 4)

        assert res == (3.5, 1.0)



    def test_middle_interval_x_border_case2(self):
        """
        testing the coordinates for the middle of the interval and the box number
        middle_interval_for_x(x, A, B, m)
        :return:
        """
        res = udfs.middle_interval_for_x(7, 3, 7, 4)

        assert res == (6.5, 4.0)



