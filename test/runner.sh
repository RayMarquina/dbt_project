#!/bin/bash

. /usr/local/bin/virtualenvwrapper.sh
workon dbt

cd /usr/src/app

if [ $# = 0 ]; then
    echo "Running all tests"
    nosetests -v --with-coverage --cover-branches --cover-html --cover-html-dir=htmlcov test/unit test/integration/* 
else
    echo "Running specified tests"
    nosetests -v --nocapture --with-coverage --cover-branches --cover-html --cover-html-dir=htmlcov $@
fi
