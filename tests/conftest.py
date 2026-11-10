import os
import sys

# Spark workers otherwise use the system python3, whose version may differ.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
