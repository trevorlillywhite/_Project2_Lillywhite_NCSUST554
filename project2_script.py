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
    
# Create two summarization methods
    # Report the min and max of a numeric column supplied by the user 
        # with one optional grouping variable
    def find_minmax(self, column: str = None, group: str = None):
        numeric_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        
        # Check that column name is a string and is in the DataFrame
        if column == None:
            pass
        elif isinstance(column, str):
            if column not in self.df.columns:
                print('Error: Invalid column (not in DataFrame)')
                return None
            else:
                # Check if column contents are numeric
                if dict(self.df.dtypes)[column] not in numeric_types:
                    print('Error: Column', column, 'is non-numeric.', 
                          'Provide a numeric column.')
                    return None
        else:
            print('Error: Column name must be a string')
            return None
        
        # Check that group name is a string and is in the DataFrame
        if group == None:
            pass
        elif isinstance(group, str):
            if group not in self.df.columns:
                print('Error: Invalid group (not in DataFrame)')
                return None
        else:
            print('Error: Group name must be a string')
            return None
                
        # Execute method: Find min/max of specified column. Group if specified.
            # Find min/max of all numeric columns if no column is specified. 
        if column != None:
            if group == None:
                max = self.df.agg(F.max(column)).collect()[0][0]
                min = self.df.agg(F.min(column)).collect()[0][0]
                data = {'Min': [min], 
                        'Max': [max]}
                p_df = pd.DataFrame(data, index = [column])
                return p_df
            else:
                max = pd.DataFrame(self.df.groupBy(group).agg(F.max(column))\
                        .alias(column+'_max').collect()[:][:])
                min = pd.DataFrame(self.df.groupBy(group).agg(F.min(column))\
                        .alias(column+'_min').collect()[:][:])
                data = list(zip(min[1], max[1]))
                p_df = pd.DataFrame(data,
                                    index=max.iloc[:,0],
                                    columns=[column+'_min', column+'_max'])\
                         .sort_index()
                p_df.index.name = group
                return p_df
            
        # If no column name is provided, calculate min/max for each numeric column
        else:
            # Determine which columns are numeric (will iterate over this list)
            list_numeric_col = []
            for col, typ in self.df.dtypes:
                if typ in numeric_types:
                    list_numeric_col.append(col)
                            
            if group == None:
                p_df = pd.DataFrame()
                for col in list_numeric_col:
                    max = self.df.agg(F.max(col)).collect()[0][0]
                    min = self.df.agg(F.min(col)).collect()[0][0]
                    data = pd.DataFrame([[min, max]], index=[col])
                    p_df = pd.concat([p_df, data])
                p_df.columns = ['min', 'max']
                return p_df
                
            else:  # NEED TO DO!!!!!!!!!!
                
                
                
                max = pd.DataFrame(self.df.groupBy(group).agg(F.max(column))\
                        .alias(column+'_max').collect()[:][:])
                min = pd.DataFrame(self.df.groupBy(group).agg(F.min(column))\
                        .alias(column+'_min').collect()[:][:])
                data = list(zip(min[1], max[1]))
                p_df = pd.DataFrame(data,
                                    index=max.iloc[:,0],
                                    columns=[column+'_min', column+'_max'])\
                         .sort_index()
                p_df.index.name = group
                return p_df
            print('TBD')
            
        
    
if __name__=="__main__":
    main()