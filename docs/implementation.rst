Implementation
==============




The surface of interest is being partitioned by the rectangular grid, with the number of cells  along latitude and longitude as parameters.
For all trajectory's points within the cell  we calculate the mean value and put it into  the center of the cell.

For one dimensional cell, the procedure for mean value :math:`x_{mean}` calculation will look like.

.. math::
    A=2; B=8; m=6; x=5.43

    dx = (B - A)/m =1

    i = ceil((x - A) / dx) = 4

    x_{mean}= A + dx * (i - 0.5) = 2+3.5=5.5


..  image:: pics/middlex.png
  :width: 400
  :alt: Alternative text

Therefore, for :math:`x_{mean} = 5.5, \forall x\in[5,6]`.

For the corresponding timestamps we calculate the mean value as well.

.. math::

    (x_{cell^j}, y_{cell^j}) = \frac{1}{k}\sum_{i=1,k}{x^i_{cell^j}, y^i_{cell^j}},

    t_{cell^j} = \frac{1}{k}\sum_{i=1,k}{t^i_{cell^j}},



where :math:`j\in[1, n\times m]`,  `k` -- is the number of points for the trajectory
(and timestamps) while object was within cell `j`, and (`n, m`) are the number of
splits along latitude and longitude.


One can see  rectangular cells with the points inside at the figure below. Some points are darker than the other,
meaning that that particular cell  been visited more than once.


..  image:: pics/trajectory_grid.png
  :width: 400
  :alt: Alternative text


The schema for the PySpark DataFrame after that transformation looks like the following:

.. code-block:: console

     root
     |-- id: integer (nullable = true)
     |-- lat_idx: integer (nullable = true)     // index of the cell along latitude
     |-- lon_idx: integer (nullable = true)     // index  of the  cell along longitude
     |-- helper: long (nullable = true)
     |-- avg_ts: timestamp (nullable = true)    // average time for  the timestamps of points in the  same  cell
     |-- lon_middle: float (nullable = true)    // average longitude
     |-- lat_middle: float (nullable = true)    //  and average  latitude as the center of the cell


State vectors and Transition  Matrix
------------------------------------


The :math:`n\times m` cells form :math:`n\times m`  states of the Markov process.

The transition between cell along the trajectory corresponds to the  move  between states.

The vector describing the state of the system in the cell :math:`(i,j)` has length of :math:`n\times m`,
and the only non-zero element in this vector  is located at  the :math:`m(i-1) + j` position.

For example, on the figure below, the blackened cell will correspond to the nonzero entry at :math:`(i,j)=(3,3)=>m(i-1) + j = 4*2+3=11`.

..  image:: pics/grid.png
  :width: 400
  :alt: Alternative text



The transition matrix is the square matrix, whose  elements :math:`a(i,j)` are  the probabilities of      moving from the state
`i` to the state `j`. Since the length of the state vector is :math:`n\times m`,  the shape of  the transition matrix  is
:math:`(nm\times nm)`.



State vector for distributed state and Transition Matrix
--------------------------------------------------------


In  the text above we assumed that the object  is located in the single cell, e.g. :math:`c(i,j)`.
The more general case is to assume that the location of the object is not known exactly, but with certain probability.
For example, one may think that the probability of the object location is smeared out over the 9 cells:

.. math::

    \sum_{k=1}^{9}p_{k}(c_{ij}) = 1,


where :math:`p_{k}(c_{ij})` is the probability of the object to be located at the `k`-th neighbour of  the central
cell :math:`c_{ij}`, and summing is done for all the neighbours.


In  this case the system is in the distributed state, and it's state vector has 9 nonzero entries, summing up to 1.

