Implementation
==============




The surface of interest is being partitioned by the rectangular grid, with the number of cells  along latitude and longitude as parameters.
For all trajectory's points within the cell  we calculate the mean value and put it into  the center of the cell.

For one dimensional cell, the procedure for mean value :math:`x_{mean}` calculation will look like.

.. math::
    A=2; B=14; m=6; x=8.43

    dx = (B - A)/m =2

    i = ceil((x - A) / dx) = 4

    x_{mean}= A + dx * (i - 0.5) = 2+2*3.5=9


..  image:: pics/middlex.png
  :width: 400
  :alt: Alternative text

Therefore,  :math:`x_{mean} = 9, \forall x\in[8,10]`, taking care of border cases.

After tesselating  the surface into :math:`x\times m` rectangulars, and projecting the coordinates into the  center of cell,
we calculate the timestamp for that cell as mean of the timestamps, while the object was within the  cell.

.. math::

    (x^j, y^j) = (x^j_{mean}, y^j_{mean}),

    t^j = \frac{1}{k}\sum_{i=1,k}{t^j_i},



where :math:`\forall j\in[1, n\times m]`,  `k` -- is the number of points of the trajectory
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


All :math:`n\times m` cells form a state vector of  :math:`n\times m`  length in the Markov process.

Transition between cells along the trajectory corresponds to   move  between states.

The only non-zero element in this vector  is located at  the :math:`m(i-1) + j` position.

For example, on the figure below, the blackened cell will correspond to the nonzero entry at :math:`(i,j)=(3,3)=>m(i-1) + j = 4*2+3=11`.

..  image:: pics/grid.png
  :width: 400
  :alt: Alternative text



The transition matrix is the square matrix, whose  elements :math:`a(i,j)` are  the probabilities of      moving from the state
`i` to the state `j`. Since the length of the state vector is :math:`n\times m`,  the shape of  the transition matrix  is
:math:`(nm\times nm)`.



State vector for "distributed state" and Transition Matrix
----------------------------------------------------------


In  the text above we assumed that the object  is located in the single cell, e.g. :math:`c(i,j)`, where `i` and `j` are the indexes along latitude and longitude correspondingly.

A more general case is to assume that the location of the object is not known exactly, but with certain probability.
For example, one may think that the probability of the object location is smeared out over  `q` cells around a central cell,
:math:`c_{ij}`:

.. math::

    \sum_{k=1}^{q}p_{k}(c_{ij}) = 1,


where :math:`p_{k}(c_{ij})` is the probability of the object to be located at the `k`-th neighbour of  the central
cell :math:`c_{ij}`, and summing is done for all the neighbours, including  the central  cell.


In  this case the system is in a **distributed state**, and it's state vector has `q` nonzero entries (out of :math:`n\times m`), summing up to 1.

A transition  matrix in this case will have the same meaning, but will have move complex structure.
Lets assume that `q=9`, i.e. the state will be distributed among 9 cells on the plane surface:


    +-------------+------------+------------+
    |`(i-1, j-1)` | `(i-1, j)` |`(i-1, j+1)`|
    +-------------+------------+------------+
    | `(i,j-1)`   |  `(i,j)`   | `(i,j+1)`  |
    +-------------+------------+------------+
    | `(i+1,j-1)` |  `(i+1,j)` | `(i+1,j+1)`|
    +-------------+------------+------------+

    with :math:`(i,j)` being location of a central cell.


1D representation
+++++++++++++++++

From 2D notion we will move to 1D:

:math:`x = \{\dots \\ m(i-1)+j-1, m(i-1)+j, m(i-1)+j+1 \dots \\ mi+j-1, mi+j,  mi+j+1, \dots \\ m(i+1)+j-1, m(i+1)+j, m(i+1)+j+1 \\
\dots\}`



where `m` -- the number  of columns, and :math:`\lvert x \rvert = m\times n`.

Those 9  states will give rise to :math:`9\times 9=81` entries in the transition matrix between two states with central cells
:math:`(i_1, j_1) (blue)\to (i_2, j_2) (red)`.

Since :math:`\sum x_1  =1`  and :math:`\sum x_2  =1`, so for  their outer product is valid :math:`\sum(x_1\otimes x_2)  =1`,
where :math:`x_1` and :math:`x_2` are the :math:`\lvert x_1 \rvert = \lvert x_2 \rvert =  m\times n` distributed  state vectors.


..  image:: pics/od_distr.png
  :width: 400
  :alt: Alternative text

The gap of length `m` is shown with the curly bracket. For `q=1` that is than the distribution is a Dirak's delta function,
a state would be described by 1 number and the transition by 1 number as well. For case `q=25` (two layers of neghbours),
the state would be given by 25 numbers, and the transition by 625 numbers.


Contributions  from consecutive transitions are accumulated into the Transition Matrix. Each set of 9 dots  in reality corresponds to 81, as shown in the inset.

..  image:: pics/tm_example.png
  :width: 500
  :alt: Alternative text
