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
        df_from_csv = session.read.load(filepath,
                             format = 'csv', 
                             sep = ',',
                             inferSchema = 'True',
                             header = 'True')
        return cls(df_from_csv)
    
    @classmethod
    def from_pandas(cls, session, pandas_df):
        # Create SQL DataFrame from pandas DataFrame provided
        df_from_pandas = session.createDataFrame(pandas_df)
        return cls(df_from_pandas)
    
# Create three validation methods
    # Validation Method 1: are column values within specified limits (inclusive)?
    def check_within_limits(self, column: str, 
                            lower: float = None, 
                            upper: float = None):
        # Check bounds for validity
            # At least one bound must be specified. 
            # Specified bounds must be numeric (not strings)
            # If both are specified, upper must be greater than lower
        if upper == None and lower == None:
            print('Error: Must specify at least one upper or lower bound.')
            return None
        elif isinstance(upper, str) or isinstance(lower, str):
            print('Error: Bounds must be numeric.')
            return None
        elif lower == upper:
            print('Error: Bounds must differ.')
            return None
        elif not (upper == None or lower == None):
            if lower > upper:
                print('Error: Lower bound cannot be greater than upper bound')
                return None
        
        # Check that column name is a string and is in the DataFrame
        if isinstance(column, str):
            if column not in self.df.columns:
                print('Error: Invalid column (not in DataFrame)')
                return None
        else:
            print('Error: Column name must be a string')
            return None
        
        # Check if column contents are non-numeric
        numeric_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        if dict(self.df.dtypes)[column] not in numeric_types:
            print('Warning: Column', column, 'is non-numeric.', 
                  'Limit check was not performed.')
            return self
        
        # Set pseudo-bounds if not specified in argument
        #    (enables .between() method)
        if upper == None:
            upper = self.df.select(F.max(column)).collect()[0][0]
        if lower == None:
            lower = self.df.select(F.min(column)).collect()[0][0]
            
        # Execute method: add new column with boolean results of .between() method
        self.df = self.df.withColumn(column + '_within_limits',
                                     F.col(column).between(lower,upper))
        return self
    
    # Validation Method 2: Are values in a string column within a user-specified
        # set of levels (list)?
    def check_string(self, column: str, 
                     levels: list[str]):
        
        # Check that column name is a string and is in the DataFrame
        if isinstance(column, str):
            if column not in self.df.columns:
                print('Error: Invalid column (not in DataFrame)')
                return None
        else:
            print('Error: Column name must be a string')
            return None
    
        # Check if column is string type
        if dict(self.df.dtypes)[column] != 'string':
            print('Warning: Column', column, 'is not string type.') 
            return self
        
                    
        # Execute method: add new column with boolean results if value in list
        self.df = self.df.withColumn(column + '_value_in_list',
                                     F.col(column).isin(levels))
        
        return self
        
    # Validation Method 3: Is each value in a column Null?
    def check_Null(self, column: str):
        
        # Check that column name is a string and is in the DataFrame
        if isinstance(column, str):
            if column not in self.df.columns:
                print('Error: Invalid column (not in DataFrame)')
                return None
        else:
            print('Error: Column name must be a string')
            return None        
                    
        # Execute method: add new column with boolean results if value is Null
        self.df = self.df.withColumn(column + '_is_Null',
                                     F.col(column).isNull())
        
        return self
    
if __name__=="__main__":
    main()