=====
Usage
=====

To use Distributed Trajectories in a project::

    import distributed_trajectories

Once installed, from the repo root launch tests.

.. code-block:: console

    ❯ ls
    AUTHORS.rst              LICENSE                  README.rst               docs                     setup.cfg                tox.ini
    CONTRIBUTING.rst         MANIFEST.in              data                     requirements.txt         setup.py
    HISTORY.rst              Makefile                 distributed_trajectories requirements_dev.txt     tests

    ❯ pytest -v
    ================================================================= test session starts =================================================================
    platform darwin -- Python 3.9.2, pytest-6.2.2, py-1.10.0, pluggy-0.13.1 -- /Users/alexey/Documents/projects/beijing_taxi_spark/taxi/bin/python3
    cachedir: .pytest_cache
    rootdir: /Users/alexey/Documents/projects/beijing_taxi_spark/distributed_trajectories
    collected 6 items

    tests/test_udfs.py::TestBasic::test_d1_state_vector_1 PASSED                                                                                    [ 16%]
    tests/test_udfs.py::TestBasic::test_d1_state_vector_2 PASSED                                                                                    [ 33%]
    tests/test_udfs.py::TestBasic::test_d1_state_vector_3 PASSED                                                                                    [ 50%]
    tests/test_udfs.py::TestBasic::test_middle_interval_x_1 PASSED                                                                                  [ 66%]
    tests/test_udfs.py::TestBasic::test_middle_interval_x_border_case1 PASSED                                                                       [ 83%]
    tests/test_udfs.py::TestBasic::test_middle_interval_x_border_case2 PASSED                                                                       [100%]

    ================================================================== 6 passed in 0.05s ==================================================================


Descend into the  project dir and launch the  code as:

.. code-block:: console

    ❯ cd distributed_trajectories
    ❯ python3 distributed_trajectories.py