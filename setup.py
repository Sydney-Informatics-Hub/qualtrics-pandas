#!/usr/bin/env python

import os
import sys
from setuptools import setup


def setup_package():
    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)

    try:
        # See setup.cfg
        setup(name='qualtrics-pandas',
              version='0.1',
              py_modules=['qualtricspd'],
              install_requires=['pandas'])
    finally:
        del sys.path[0]
        os.chdir(old_path)
    return


if __name__ == '__main__':
    setup_package()
