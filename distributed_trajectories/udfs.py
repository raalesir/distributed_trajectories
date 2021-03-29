"""
    User Defined Functions
    ======================
"""

from math import  ceil
import   numpy as np



def middle_interval_for_x(x, A, B, m):
    """
    given the borders `[A,B]` and the number of intervals `m`,
    calculates the new `x` as  the coordinate of the middle of the interval `x`
    belongs to.
    """

    if (x == A):
        x = x + .001
    elif (x == B):
        x = x - .001

    dx = (B - A) / m
    i = ceil((x - A) / dx)

    return A + dx * (i - 0.5), float(i)


def d2_coords(i, j, width=1):
    """
    given the (i,j) get the  coords of the neighbouring cells within `width`
    """
    d2 = [(x, y) for x in range(i - width, i + width + 1) for y in range(j - width, j + width + 1)]

    return d2


def make_mesh(width=1):
    """
    create a  mesh of given `width`
    """
    X, Y = np.meshgrid(
        np.linspace(-1,1, 2*width+1), np.linspace(-1,1,2*width+1)
    )

    return X, Y


def put_gauss_on_mesh(X, Y, mu=0, sigma=2):
    """
    Given the mesh, produces a Guass-like bell on top of it,
    and returns a list  of unrolled 2D array
    """

    G = np.exp(-((X - mu) ** 2 + (Y - mu) ** 2) / 2.0 * sigma ** 2)
    G = G.round(3)
    G = G / np.sum(G)
    G = G.round(3)

    vals = [float(G[i, j]) for i in range(len(G)) for j in range(len(G))]

    return vals


def d1_coords(d2_vals, m):
    """
    transform  2D to  1D coords
    """

    # d1_vals = [[float((coords[0] - 1) * m + coords[1]), val] for coords, val in d2_vals]
    d1_vals = [[float(coords[0] * m + coords[1]), val] for coords, val in d2_vals]

    return d1_vals



def d1_coords_pure(d2_c, m):
    """
    transform  2D to  1D coords without values
    """

    d1_vals = [ [float(coords[0]* m + coords[1])] for coords in d2_c ]

    return d1_vals


def d1_state_vector(i,j, width, m, n):
    """
    given `i` and `j` positions on the lattice and the width of distribution,
    produces a 1D state vector representation of `mxn` length.
    """


    if check_central_position(m, n, i, j, width):

        d2 = d2_coords(i, j, width)
        # print(d2)
        X,Y = make_mesh(width)
        vals = put_gauss_on_mesh(X,Y)
        # print(vals)
        # vals = [ 0.011, 0.084,  0.011, 0.084, 0.62, 0.084, 0.011, 0.084, 0.011]
        d2_vals = list(zip(d2,vals))


        d1_vals = d1_coords(d2_vals, n)
#         print(type(d1_vals[0]))
    else:
        # print("the distribution is out  of the grid")
        d1_vals = []
    return d1_vals


def check_central_position(m, n, i, j, width):
    """
    checking if  the  distribution given by the   `i,j` and  `width`  will be inside the  grid
    defined  by the `n` and `m`.

    :math:`i\in[1..m]`, :math:`j\in[1..n]`, where `m` -- number of cells along latitude  and
     `n` is the number of cells along longitude.

    :param m: number of cells along latitude
    :param n: number of cells along  longitude
    :param i: current position along latitude
    :param j: current position along longitude
    :param width: width of the distribution of state
    :return: True or False

    """


    result = (i + width < m) & (i - width >= 0) & (j + width < n) & (j - width >= 0)

    return result


def updates_to_the_transition_matrix(state1, state2):
    """
    produces an update to the transition matrix due  to transition from `state1` to `state2`
    Returns list of tuples
    """

    update = [[float(el1[0]), float(el2[0]), el1[1] * el2[1]]
              for el1 in state1 for el2 in state2]

    return update



def filter_OD(origins, destinations):
    """
    takes lists of origins and destinations in (1D notation)
    and returns list of tuples with OD coorinates
    """
    if len(origins) == len(destinations):
        return list(zip(origins, destinations))

    else:
        return []