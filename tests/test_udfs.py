

"""
=========================================
Tests for `distributed_trajectories.udfs`
=========================================
"""

from distributed_trajectories import udfs


class TestBasic:
    """
    Testing UDFs
    """
    def test_d1_state_vector_1(self):
        """
        testing the transformation to 1D coordinates
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=4, width=1, m=7, n=6)
        res = [el[0] for el in res]

        assert res == [9.0, 10.0, 11.0, 15.0, 16.0, 17.0, 21.0, 22.0, 23.0]


    def test_d1_state_vector_2(self):
        """
        testing the transformation to 1D coordinates with `width==0`
        :return:
        """
        res = udfs.d1_state_vector(i=2, j=5, width=0, m=7, n=7)
        res = [el[0] for el in res]

        assert res == [19.0]

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

        assert res == (4.5, 2.0)

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

    def test_d2_coords_no_width(self):
        """
        testing 2D coords creation for given width

        :return:  Truee or False
        """

        res = udfs.d2_coords(1, 3, 0)

        assert res == [(1, 3)]


    def test_d2_coords_width(self):
        """
        testing 2D coords creation for given width

        :return:  Truee or False
        """

        res = udfs.d2_coords(1, 3, 1)

        assert res == [(0, 2), (0, 3), (0, 4), (1, 2), (1, 3), (1, 4), (2, 2), (2, 3), (2, 4)]


    def test_d1_coords_pure(self):
        """
        transform for 2D to 1D coords
        :return:
        """
        res = udfs.d1_coords_pure([(0, 2)], 4)

        assert res == [[2.0]]


    def test_d1_coords_pure_1(self):
        """
           transform for 2D to 1D coords
           :return:
           """
        res = udfs.d1_coords_pure([(0, 2), (0, 4), (1, 3)], 5)

        assert res == [[2.0], [4.0], [8.0]]


    def test_check_central_position_pass(self):
        """
        testing if the filter for  the elements works.... Needs better docs...
        :return:
        """

        res = udfs.check_central_position(m=4, n=5, i=1, j=3, width=1)

        assert res == True


    def test_check_central_position_fail(self):
        """
        testing if the filter for  the elements works.... Needs better docs...
        :return:
        """

        res = udfs.check_central_position(m=4, n=5, i=1, j=4, width=1)

        assert res == False


    def test_filter_OD_pass(self):
        """

        :return:
        """

        res = udfs.filter_OD([1,2,3], [4,5,6])

        assert res == [(1, 4), (2, 5), (3, 6)]


    def test_filter_OD_fail(self):
        """

        :return:
        """

        res = udfs.filter_OD([1, 2, 3], [4, 5])

        assert res == []
