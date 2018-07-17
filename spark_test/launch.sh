#!/bin/bash

cd ../
rm spectractor.zip
zip -r spectractor.zip spectractor
cd spark_test
PYSPARK_DRIVER_PYTHON_OPTS="/Users/julien/anaconda3/bin/jupyter-notebook" /Users/julien/Documents/workspace/lib/spark/bin/pyspark --py-files /Users/julien/Documents/workspace/otrepos/Spectractor/spectractor.zip
