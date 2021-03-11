.. highlight:: shell

============
Installation
============


Stable release
--------------

To install Distributed Trajectories, run this command in your terminal:

.. code-block:: console

    $ pip install distributed_trajectories

This is the preferred method to install Distributed Trajectories, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
------------

The sources for Distributed Trajectories can be downloaded from the `Github repo`_.

#. clone the public repository:

    .. code-block:: console

        $ git clone git://github.com/raalesir/distributed_trajectories
#. create a Python virtual environment

    .. code-block:: console

        $ cd distributed_trajectories/distributed_trajectories && python3 -m venv taxi


#. activate  the ``venv``. Check ``which python`` to point to the current directory.

    .. code-block:: console

       $ source taxi/bin/activate


#. install dependencies:

    .. code-block:: console

     $ pip3  install  -r requirements.txt



.. _Github repo: https://github.com/raalesir/distributed_trajectories
.. _tarball: https://github.com/raalesir/distributed_trajectories/tarball/master


Project  structure
-------------------

Once cloned, the  repo structure  should look like:

.. code-block:: console

    ❯ tree -L 2
    .
    ├── AUTHORS.rst
    ├── CONTRIBUTING.rst
    ├── HISTORY.rst
    ├── LICENSE
    ├── MANIFEST.in
    ├── Makefile
    ├── README.rst
    ├── data                   //small sample of data, 10 random trajectories
    │   └── data.tar.gz
    ├── distributed_trajectories        // main package
    │   ├── OD.py
    │   ├── TM.py
    │   ├── __init__.py
    │   ├── cli.py
    │   ├── consts.py
    │   ├── distributed_trajectories.py
    │   └── udfs.py
    ├── docs                        // documentation  source
    │   ├── Makefile
    │   ├── authors.rst
    │   ├── conf.py
    │   ├── contributing.rst
    │   ├── history.rst
    │   ├── index.rst
    │   ├── installation.rst
    │   ├── make.bat
    │   ├── modules.rst
    │   ├── readme.rst
    │   └── usage.rst
    ├── requirements.txt
    ├── requirements_dev.txt
    ├── setup.cfg
    ├── setup.py
    ├── tests                           // tests  to run
    │   ├── __init__.py
    │   └── test_udfs.py
    └── tox.ini