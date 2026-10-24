import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "src", "hr", "gold"))
sys.path.insert(0, os.path.join(ROOT, "src", "hr", "generator"))
sys.path.insert(0, os.path.join(ROOT, "src", "orm", "gold"))
sys.path.insert(0, os.path.join(ROOT, "src", "orm", "generator"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]")
         .appName("edp-unit-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
