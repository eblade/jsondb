#!/bin/bash
conda install pyflakes pip pep8 coverage
pip install pytest pytest-pep8 pytest-cov pytest-flakes
pip install https://github.com/eblade/blist/archive/master.zip
