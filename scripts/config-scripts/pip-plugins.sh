#!/bin/bash

echo "Install Medusa 1.0 plugins"
pip install numpy
pip install statsmodels
pip install scipy
pip install celery
pip install paramiko
pip install fabric
pip install psutil
pip install simplejson
pip install paramiko
# pip install 'celery==3.1.20'
pip install celery
pip install fabric
pip install nose
pip install dogpile.cache
pip install -U amqp
pip install flower
# install hadoopy
pip install -e git+https://github.com/bwhite/hadoopy#egg=hadoopy
pip install bpython

