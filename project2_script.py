# Project 2 - NCSU ST 554
# Author: Trevor Lillywhite
# Due Date: March 24, 2026

# Part I - Creating a Class
#    In this part, we will create a .py script with a custom class
#    This script will wrap a Spark (SQL style) DataFrame and provide
#      functionality for cleaning and checking the data

# Import relevant modules
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

# Create a class called SparkDataCheck
class SparkDataCheck():
    # Initialize with a DataFrame
    def __init__(self, df: DataFrame):     
        # Create .df attribute with the df argument
        self.df = df
        
