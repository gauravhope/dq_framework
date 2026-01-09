# Objective:
# Read DQ rules from YAML and return Spark DataFrame

import yaml
from pyspark.sql import SparkSession

def load_dq_rules(spark: SparkSession, yaml_path: str):
    with open(yaml_path, "r") as f:
        rules = yaml.safe_load(f)["rules"]

    return spark.createDataFrame(rules)
