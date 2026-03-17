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
        
    # Create two @classmethods: .from_csv() and .from_pandas()
    @classmethod
    def from_csv(cls, session, filepath):
        # Create DataFrame from csv using file path provided
        df = session.read.load(filepath,
                             format = 'csv', 
                             sep = ',',
                             inferSchema = 'True',
                             header = 'True')
        return cls(df)
    
    @classmethod
    def from_pandas(cls, session, pandas_df):
        # Create SQL DataFrame from pandas DataFrame provided
        df = session.createDataFrame(pandas_df)
        return cls(df)
    

    
    
    
    
    
if __name__=="__main__":
    main()