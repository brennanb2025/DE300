from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
#from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
from sqlalchemy import create_engine

import pyspark
from pyspark.sql.functions import col, lit, monotonically_increasing_id
from pyspark.ml.classification import LogisticRegression as LogisticRegressionSpark, LinearSVC
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.sql.window import Window
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import functions as F

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression as SklearnLogisticRegression
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split


from scrapy import Selector
import requests

import random as rd

import numpy as np


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when, isnan, isnull, count, avg, trim, mean, rand
from pyspark.sql.functions import sum as spark_sum


from pyspark.ml.linalg import VectorUDT


# Register UDF
from pyspark.sql.functions import udf



# I tried for so long to make the spark machine learning functions work.
# But I kept getting this error:
"""
Traceback (most recent call last):
File "/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 433, in _execute_task
    result = execute_callable(context=context, **execute_callable_kwargs)
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 199, in execute
    return_value = self.execute_callable()
                ^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 216, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/dags/brennan_hw4.py", line 1106, in logistic_train_spark_func
    log_reg = LogisticRegressionSpark(labelCol='target', featuresCol='features', maxIter=1000)
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/__init__.py", line 139, in wrapper
    return func(self, **kwargs)
        ^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/classification.py", line 1317, in __init__
    super(LogisticRegression, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/wrapper.py", line 49, in __init__
    super(JavaWrapper, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/classification.py", line 1008, in __init__
    super(_LogisticRegressionParams, self).__init__(*args)
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 151, in __init__
    super(HasProbabilityCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 512, in __init__
    super(HasThresholds, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 174, in __init__
    super(HasRawPredictionCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 105, in __init__
    super(HasLabelCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 82, in __init__
    super(HasFeaturesCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 128, in __init__
    super(HasPredictionCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 60, in __init__
    super(HasRegParam, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 443, in __init__
    super(HasElasticNetParam, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 38, in __init__
    super(HasMaxIter, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 466, in __init__
    super(HasFitIntercept, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 354, in __init__
    super(HasTol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 489, in __init__
    super(HasStandardization, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 557, in __init__
    super(HasWeightCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 624, in __init__
    super(HasAggregationDepth, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 534, in __init__
    super(HasThreshold, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 782, in __init__
    super(HasMaxBlockSizeInMB, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/__init__.py", line 269, in __init__
    self._copy_params()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/__init__.py", line 276, in _copy_params
    src_name_attrs = [(x, getattr(cls, x)) for x in dir(cls)]
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/__init__.py", line 276, in <listcomp>
    src_name_attrs = [(x, getattr(cls, x)) for x in dir(cls)]
                        ^^^^^^^^^^^^^^^
AttributeError: __provides__
"""

# and this error:
"""
Traceback (most recent call last):
File "/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 433, in _execute_task
    result = execute_callable(context=context, **execute_callable_kwargs)
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 199, in execute
    return_value = self.execute_callable()
                ^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 216, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/dags/brennan_hw4.py", line 1149, in svm_train_spark_func
    assembler = VectorAssembler(inputCols=features, outputCol='features')
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/__init__.py", line 139, in wrapper
    return func(self, **kwargs)
        ^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/feature.py", line 5357, in __init__
    super(VectorAssembler, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/wrapper.py", line 49, in __init__
    super(JavaWrapper, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 219, in __init__
    super(HasInputCols, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 241, in __init__
    super(HasOutputCol, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/shared.py", line 421, in __init__
    super(HasHandleInvalid, self).__init__()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/__init__.py", line 269, in __init__
    self._copy_params()
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/__init__.py", line 276, in _copy_params
    src_name_attrs = [(x, getattr(cls, x)) for x in dir(cls)]
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/local/airflow/.local/lib/python3.11/site-packages/pyspark/ml/param/__init__.py", line 276, in <listcomp>
    src_name_attrs = [(x, getattr(cls, x)) for x in dir(cls)]
                        ^^^^^^^^^^^^^^^
AttributeError: __provides__
"""
# This is just on the VectorAssembler constructor and the LogisticRegression functions
# From pyspark. ChatGPT told me that the installation was likely corrupted.
# So, I'm using Pandas' machine learning stuff.
# The code I would've used is below each pandas section.
# I also talked to Robert about it, and he didn't know the issue.







# read the parameters from toml
CONFIG_BUCKET = "de300spring2024-brennanb-airflow"
CONFIG_FILE_KEY = "config_hw4.toml"



TABLE_NAMES = {
    "original_data": "heart_disease",
    "clean_data_pandas": "heart_disease_clean_data_pandas",
    "clean_data_spark": "heart_disease_clean_data_spark",
    "train_data_pandas": "heart_disease_train_data_pandas",
    "test_data_pandas": "heart_disease_test_data_pandas",
    "train_data_spark": "heart_disease_train_data_spark",
    "test_data_spark": "heart_disease_test_data_spark",
    # "normalization_data": "heart_disease_normalization_values",
    "max_fe": "heart_disease_max_fe_features",
    "product_fe": "heart_disease_product_fe_features"
}

ENCODED_SUFFIX = "_encoded"

# Define the default args dictionary for DAG
default_args = {
    'owner': 'BrennanBenson',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}


def read_config_from_s3() -> dict:
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_FILE_KEY)
    config_data = obj['Body'].read().decode('utf-8')
    params = tomli.loads(config_data)
    return params


# Usage
PARAMS = read_config_from_s3()

def create_db_connection():
    """
    Create a database connection to the PostgreSQL RDS instance using SQLAlchemy.
    """
   
    conn_uri = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"

    # Create a SQLAlchemy engine and connect
    engine = create_engine(conn_uri)
    connection = engine.connect()

    return connection



def from_table_to_df(input_table_names: list[str], output_table_names: list[str]):
    """
    Decorator to open a list of tables input_table_names, load them in df and pass the dataframe to the function; on exit, it deletes tables in output_table_names
    The function has key = dfs with the value corresponding the list of the dataframes 

    The function must return a dictionary with key dfs; the values must be a list of dictionaries with keys df and table_name; Each df is written to table table_name
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            import pandas as pd

            """
            load tables to dataframes
            """
            if input_table_names is None:
                raise ValueError('input_table_names cannot be None')
            
            _input_table_names = None
            if isinstance(input_table_names, str):
                _input_table_names = [input_table_names]
            else:
                _input_table_names = input_table_names

            import pandas as pd
            
            print(f'Loading input tables to dataframes: {_input_table_names}')

            # open the connection
            conn = create_db_connection()

            # read tables and convert to dataframes
            dfs = []
            for table_name in _input_table_names:
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                dfs.append(df)

            if isinstance(input_table_names, str):
                dfs = dfs[0]

            """
            call the main function
            """

            kwargs['dfs'] = dfs
            kwargs['output_table_names'] = output_table_names
            result = func(*args, **kwargs)

            """
            delete tables
            """

            print(f'Deleting tables: {output_table_names}')
            if output_table_names is None:
                _output_table_names = []
            elif isinstance(output_table_names, str):
                _output_table_names = [output_table_names]
            else:
                _output_table_names = output_table_names
            
            print(f"Dropping tables {_output_table_names}")
            for table_name in _output_table_names:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            """
            write dataframes in result to tables
            """

            for pairs in result['dfs']:
                df = pairs['df']
                table_name = pairs['table_name']
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                print(f"Wrote to table {table_name}")

            conn.close()
            result.pop('dfs')

            return result
        return wrapper
    return decorator



def add_data_to_table_func(**kwargs):
    """
    Insert data from a CSV file stored in S3 to a database table.
    """
    # Create a database connection
    conn = create_db_connection()
    
    # Set the S3 bucket and file key
    s3_bucket = PARAMS['files']['s3_bucket']
    s3_key = PARAMS['files']['s3_file_key']
    
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    # Get the object from the S3 bucket
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    
    # Read the CSV file directly from the S3 object's byte stream into a DataFrame
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content), nrows=899)
    df = df.rename(columns={'ekgday(day':'ekgday'})
    
    # Write the DataFrame to SQL
    df.to_sql(TABLE_NAMES['original_data'], con=conn, if_exists="replace", index=False)

    s3_client.download_file(s3_bucket, s3_key, "/tmp/heart_disease_brennan.csv")

    # Close the database connection
    conn.close()

    return {'status': 1}

# @from_table_to_df(TABLE_NAMES['original_data'], None)
# def clean_data_func(**kwargs):
#     """
#     data cleaning: drop none, remove outliers based on z-scores
#     apply label encoding on categorical variables: assumption is that every string column is categorical
#     """

#     import pandas as pd
#     from sklearn.preprocessing import LabelEncoder

#     data_df = kwargs['dfs']

#     # Drop rows with missing values
#     data_df = data_df.dropna()

#     # Remove outliers using Z-score 
#     numeric_columns = [v for v in data_df.select_dtypes(include=['float64', 'int64']).columns if v != PARAMS['ml']['labels']]
#     for column in numeric_columns:
#         values = (data_df[column] - data_df[column].mean()).abs() / data_df[column].std() - PARAMS['ml']['outliers_std_factor']
#         data_df = data_df[values < PARAMS['ml']['tolerance']]

#     # label encoding
#     label_encoder = LabelEncoder()
#     string_columns = [v for v in data_df.select_dtypes(exclude=['float64', 'int64']).columns if v != PARAMS['ml']['labels']]
#     for v in string_columns:
#         data_df[v + ENCODED_SUFFIX] = label_encoder.fit_transform(data_df[v])

#     return {
#         'dfs': [
#             {'df': data_df, 
#              'table_name': TABLE_NAMES['clean_data']
#              }]
#         }


@from_table_to_df(TABLE_NAMES['original_data'], None)
def clean_data_pandas_func(**kwargs):


    print("Doing clean/impute pandas!")

    import pandas as pd
    import numpy as np

    df = kwargs['dfs']

    # The column pncaden is completely empty.
    # According to the data source, restckm and exerckm are both "irrelevant"
    # According to the data source, thalsev, thalpul, and earlobe are "not used"
    df.drop(['pncaden', 'restckm', 'exerckm', 'thalsev', 'thalpul', 'earlobe'], axis=1, inplace=True)
    
    # Next, remove features with >95% null data.
    null_percentage = (df.isnull().sum() / len(df)) * 100
    # null_percentage_sorted = null_percentage.sort_values(ascending=False)
    # null_percentage_sorted.plot(kind='bar', figsize=(12, 6))


    print("Data with null percentage higher than 95%:")
    print(null_percentage[null_percentage > 95])

    # Remove columns with null percentage > 95%. These columns do not include
    #   enough data to meaningfully be able to make a prediction.
    df.drop(['restef', 'restwm', 'exeref', 'exerwm'], axis=1, inplace=True)


    # Categorize columns
    # columns that are binary
    binary = ['sex', 'painloc', 'painexer', 'relrest', 'smoke', 'fbs', 'dm', 'famhist', 'dig', 'prop', \
            'nitr', 'pro', 'diuretic', 'exang', 'xhypo', 'htn', 'target']
    print("There are",len(binary),"binary columns.")

    # columns that are categorical
    categorical = ['cp', 'restecg', 'proto', 'slope', 'thal']
    print("There are",len(categorical),"categorical columns.")

    # columns that have only integers
    integer = ['age', 'cigs', 'years', 'ekgmo', 'ekgday', 'ekgyr', 'ca', 'cmo', 'cday', 'cyr']
    print("There are",len(integer),"integer columns.")

    # columns that have continuous data
    continuous = ['trestbps', 'chol', 'thalach', 'thalrest', 'tpeakbps', 'tpeakbpd', 'dummy', \
                'trestbpd', 'oldpeak', 'thaldur', 'thaltime', 'met', 'rldv5', 'rldv5e']
    print("There are",len(continuous),"continuous columns.")


    # Before we visualize the binary columns, there is bad data in the prop column.
    # According to the docs, prop is categorical.
    """
    prop (Beta blocker used during exercise ECG: 1 = yes; 0 = no)
    """
    # However, it includes bad data, ones that aren't in 0 or 1.
    acceptable_values = [0, 1]

    print("Number of rows with bad data in 'prop' column:",
        len(df[(~df['prop'].isnull()) & (~df['prop'].isin(acceptable_values))]))

    # we should set these to null.
    df.loc[~df['prop'].isin(acceptable_values), 'prop'] = None

    print("Number of rows with bad data in 'prop' column after imputation:",
        len(df[(~df['prop'].isnull()) & (~df['prop'].isin(acceptable_values))]))


    # Before we visualize the categorical columns, there is bad data in the proto column.
    # According to the docs, proto is categorical.
    """
    proto: exercise protocol
            1 = Bruce
            2 = Kottus
            3 = McHenry
            4 = fast Balke
            5 = Balke
            6 = Noughton
            7 = bike 150 kpa min/min  (Not sure if "kpa min/min" is what was written!)
            8 = bike 125 kpa min/min
            9 = bike 100 kpa min/min
            10 = bike 75 kpa min/min
            11 = bike 50 kpa min/min
            12 = arm ergometer
    """
    # However, it includes bad data, ones that aren't in these values.
    acceptable_values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    print("Number of rows with bad data in 'proto' column:",
        len(df[(~df['proto'].isnull()) & (~df['proto'].isin(acceptable_values))]))

    # we should set these to null.
    df.loc[~df['proto'].isin(acceptable_values), 'proto'] = None

    print("Number of rows with bad data in 'proto' column after imputation:",
        len(df[(~df['proto'].isnull()) & (~df['proto'].isin(acceptable_values))]))





    # Impute data: check outliers in continuous and integer columns (mean +- 2/3 standard deviations)
    # Impute data: remove outliers (continued)

    # After looking at the outlier data in each column, I believe the outliers
    # that are 0 in trestbps, chol, and trestbpd are bad data.
    # for these columns, it is unreasonable for the data to be 0.
    # For trestbps (resting blood pressure in mm Hg on admission to the hospital),
    #   it is unreasonable for a person's blood pressure to be 0.
    # For trestbpd (resting blood pressure), it is unreasonable for a person's
    #   resting blood pressure to be 0.
    # For chol (serum cholestoral in mg/dl), it is unreasonable for a person's
    #   cholestoral to be 0.
    # So, I will remove all 0 values in these columns.

    zero_outlier_cols = ['trestbps', 'trestbpd', 'chol']
    for col in zero_outlier_cols:
        df = df[df[col] != 0]


    # I also think the three outliers in the column 'cyr' are incorrect data.
    # It is unreasonable for the cyr (year of cardiac cath (sp?)) to not be around
    #   the other points, all around 81-86.
    # The value of 1 could be corrupted data, or it could be a typo for 81
    #   and the values of 16 and 26 are likely typos that should be 87.
    #   I believe the value of 1 is simply bad data, as all of the data with cyr=81
    #   are grouped at the top of the dataset, yet this one is right in the middle.
    #   So, I will remove this data.

    df['cyr'] = df['cyr'].replace(to_replace=1, value=None)
    df['cyr'] = df['cyr'].replace(to_replace=16, value=86)
    df['cyr'] = df['cyr'].replace(to_replace=26, value=86)

    # I believe the remaining data is true, valid data. However, I will remove
    # outliers past 2*σ to make the columns more normally distributed.
    for col in df.columns:
        if col in continuous or col in integer:
            mean = df[col].mean()
            std = df[col].std()
            df.loc[(df[col] < mean - 2 * std) | (df[col] > mean + 2 * std), col] = None




    # Impute data: remove null values

    # Replace null values with median for categorical columns
    for col in categorical:
        median_val = df[col].median()
        df[col].fillna(median_val, inplace=True)

    # Replace null values with median for binary columns
    for col in binary:
        median_val = df[col].median()
        df[col].fillna(median_val, inplace=True)

    # Replace null values with mean for integer columns
    for col in integer:
        mean_val = df[col].mean()
        df[col].fillna(mean_val, inplace=True)

    # Replace null values with mean for continuous columns
    for col in continuous:
        mean_val = df[col].mean()
        df[col].fillna(mean_val, inplace=True)

    null_percentage = (df.isnull().sum() / len(df)) * 100
    print("Number of columns that contain any null data after imputation:", len(null_percentage[null_percentage != 0]))



    #Q-Q plot imputation
    df['ca'] = np.sqrt(df['ca'])


    return {
        'dfs': [
            {'df': df, 
             'table_name': TABLE_NAMES['clean_data_pandas']
             }]
        }


import json



def scrape_smoke_func():

    # You will impute the missing values in the “smoke” column with smoking rates
    #   by age or sex.
    # You will use two different sources for obtaining these smoking rates.
    # Create a separate column for each source.
    # After imputing the missing values, apply an appropriate transform
    #   for each column


    url_1 = "https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking-and-vaping/latest-release"
    # This source lists smoking rates (current daily smokers) by age group.
    # Replace the missing values with the smoking rate in the corresponding age groups.
    response = requests.get(url_1)
    if response.status_code != 200:
        print("request failed")

    html_content = response.content
    full_sel = Selector(text=html_content)
    table_selector = full_sel.xpath("//table[caption='Proportion of people 15 years and over who were current daily smokers by age, 2011–12 to 2022']/tbody/tr")

    age_to_smoking_rate = {}
    # Iterate through rows
    for row in table_selector:

        rowValAgeRange = row.xpath('.//th[@class="row-header"]/text()').extract()[0]
        if "–" in rowValAgeRange:
            rangeAges = rowValAgeRange.split("–")
            ageRange = (int(rangeAges[0]), int(rangeAges[1]))
        else:
            ageRange = (int(rowValAgeRange[:2]), float('inf')) # first 2 chars = digits
        col2022 = row.xpath('.//td[@class="data-value"][position()=last()-2]/text()').extract()[0]

        age_to_smoking_rate[ageRange] = float(col2022)

    print("smoking rates by age (abs):",age_to_smoking_rate)


    url_2 = "https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm"
    # This source lists smoking rates by age group and by sex.
    # • For female patients, replace the missing values with the smoking rate in
    #     their corresponding age groups.
    # • For male patients, replace the missing values with
    # smoking rate in age group * (smoking rate among men / smoking rate among women)

    response = requests.get(url_2)
    if response.status_code != 200:
        print("request failed")

    html_content = response.content
    full_sel = Selector(text=html_content)
    row_selector = full_sel.xpath("//div[@class='row '][3]")

    ul_selector = row_selector.xpath("//ul[@class='block-list']")

    sex_li_text = ul_selector[0].xpath(".//li/text()") # sex from first row
    age_li_text = ul_selector[1].xpath(".//li/text()") # age from second row

    malePercent = float(sex_li_text[0].extract().split("(")[1].split("%)")[0])
    femalePercent = float(sex_li_text[1].extract().split("(")[1].split("%)")[0])

    ageDict = {}
    for age_li in age_li_text:
        ageRange = age_li.extract().split("aged ")[1].split(" years")[0]
        percentValue = float(age_li.extract().split("(")[1].split("%)")[0])
        if "–" in ageRange:
            ages = ageRange.split("–")
            ageDict[(int(ages[0]), int(ages[1]))] = percentValue
        else:
            ageDict[(int(ageRange), float('inf'))] = percentValue

    # ageDict contains the flat rates for each age group. This is what we should
    #   use to impute for female patients, but we need to change the rates for male
    #   patients according to the formula:
    #   smoking rate in age group * (smoking rate among men / smoking rate among women)

    maleSmokingRates = {}
    for k in ageDict.keys():
        maleSmokingRates[k] = ageDict[k] * (malePercent / femalePercent)

    print("male smoking rates by age (cdc):",maleSmokingRates)
    print("female smoking rates by age (cdc):",ageDict)

    jsonDict = {}
    jsonDict = {
        "cdc_male": {str(k): v for k, v in maleSmokingRates.items()},
        "cdc_female": {str(k): v for k, v in ageDict.items()},
        "abs": {str(k): v for k, v in age_to_smoking_rate.items()}
    }

    with open('/tmp/scrape_data_brennan.json', 'w') as json_file:
        json.dump(jsonDict, json_file)


def parse_tuple_key(key):
    """Convert string representation of a tuple back to a tuple."""
    key = key.strip("()")
    parts = key.split(", ")
    if len(parts) == 2:
        return int(parts[0]), float(parts[1]) if parts[1] == 'inf' else int(parts[1])
    return key

def impute_smoke_func():
    df = pd.read_csv("/tmp/heart_disease_pandas_FE1.csv")


    with open('/tmp/scrape_data_brennan.json', 'r') as json_file:
        data = json.load(json_file)

    # Convert string keys back to tuple keys
    maleSmokingRates = {parse_tuple_key(k): v for k, v in data['cdc_male'].items()}
    ageDict = {parse_tuple_key(k): v for k, v in data['cdc_female'].items()}
    age_to_smoking_rate = {parse_tuple_key(k): v for k, v in data['abs'].items()}



    # three cols --> use other two (combination or one of each) to impute the original smoke col
    # for percents just do random and see if in

    df['abs_smoke'] = 0

    # Iterate over the dictionary
    for age_range, percent_chance in age_to_smoking_rate.items():
        # Check if age falls within the range
        df['abs_smoke'] = df.apply(lambda row: 1 if row['age'] >= age_range[0] and row['age'] <= age_range[1]
                                    and rd.uniform(0, 100) <= percent_chance else row['abs_smoke'], axis=1)

    df['cdc_smoke'] = 0

    # Iterate over the dictionary
    for age_range, percent_chance in maleSmokingRates.items(): # male
        # Check if age falls within the range
        df['cdc_smoke'] = df.apply(lambda row: row['cdc_smoke'] if row['sex'] == 0 else # if female, keep as is
                                    1 if row['age'] >= age_range[0] and row['age'] <= age_range[1]
                                    and rd.uniform(0, 100) <= percent_chance else row['cdc_smoke'], axis=1)

    # for female, just use the smoking rate in age groups.
    for age_range, percent_chance in ageDict.items(): # female
        df['cdc_smoke'] = df.apply(lambda row: row['cdc_smoke'] if row['sex'] == 1 else # if male, keep as is
                                    1 if row['age'] >= age_range[0] and row['age'] <= age_range[1]
                                    and rd.uniform(0, 100) <= percent_chance else row['cdc_smoke'], axis=1)

    print(df[['age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke']].head(20))



    # Now impute the smoke column.

    # Calculate the percent for male and female
    smoke_percent_male = {}
    smoke_percent_female = {}
    smoke_starts = {}
    for k in age_to_smoking_rate.keys():
        smoke_percent_male[k] = age_to_smoking_rate[k]
        smoke_percent_female[k] = age_to_smoking_rate[k]
        smoke_starts[k[0]] = k


    for k in maleSmokingRates.keys():
        rangeStart = k[0]
        rangeEnd = k[1]
        if rangeEnd == float('inf'):
            for k_i in smoke_starts.keys(): # go thru all --> if > start, set it.
                if k_i > rangeStart:
                    # print("setting male", smoke_starts[k_i], "to", (smoke_percent_male[smoke_starts[k_i]] + maleSmokingRates[k]) / 2,
                    #           "using", smoke_percent_male[smoke_starts[k_i]], "and", maleSmokingRates[k])
                    smoke_percent_male[smoke_starts[k_i]] = (
                        smoke_percent_male[smoke_starts[k_i]] + maleSmokingRates[k]
                    ) / 2
            break
        for i in range(rangeStart, rangeEnd):
            if i in smoke_starts:
                # print("setting male", smoke_starts[i], "to", (smoke_percent_male[smoke_starts[i]] + maleSmokingRates[k]) / 2,
                #       "using", smoke_percent_male[smoke_starts[i]], "and", maleSmokingRates[k])
                smoke_percent_male[smoke_starts[i]] = (
                    smoke_percent_male[smoke_starts[i]] + maleSmokingRates[k]
                ) / 2

    for k in ageDict.keys():
        rangeStart = k[0]
        rangeEnd = k[1]
        if rangeEnd == float('inf'):
            for k_i in smoke_starts.keys(): # go thru all --> if > start, set it.
                if k_i > rangeStart:
                    # print("setting female", smoke_starts[k_i], "to", (smoke_percent_female[smoke_starts[k_i]] + ageDict[k]) / 2,
                    #           "using", smoke_percent_female[smoke_starts[k_i]], "and", ageDict[k])
                    smoke_percent_female[smoke_starts[k_i]] = (
                        smoke_percent_female[smoke_starts[k_i]] + ageDict[k]
                    ) / 2
            break
        for i in range(rangeStart, rangeEnd):
            if i in smoke_starts:
                # print("setting female", smoke_starts[i], "to", (smoke_percent_female[smoke_starts[i]] + ageDict[k]) / 2,
                #       "using", smoke_percent_female[smoke_starts[i]], "and", ageDict[k])
                smoke_percent_female[smoke_starts[i]] = (
                    smoke_percent_female[smoke_starts[i]] + ageDict[k]
                ) / 2

    print("Imputing null 'smoke' values with:")
    print("Age smoke percent male:",smoke_percent_male)
    print("Age smoke percent female:",smoke_percent_female)



    # Function to impute null values in 'smoke' column based on age and sex
    def impute_smoke(row):
        if pd.isnull(row['smoke']):  # only impute if null
            age = row['age']
            sex = row['sex']
            # Get corresponding age smoke percent based on sex
            age_smoke_percent = smoke_percent_male if sex == 1 else smoke_percent_female
            # Get random percentage for imputation
            random_percentage = rd.uniform(0, 100)
            # If age falls within any range, impute with the corresponding percentage
            for age_range, percent in age_smoke_percent.items():
                if age >= age_range[0] and age <= age_range[1]:
                    return 1 if random_percentage <= percent else 0
        return row['smoke']  # If 'smoke' value is not null, return original value

    df['smoke'] = df.apply(impute_smoke, axis=1)

    print("Finished imputing values in the 'smoke' column.")



    null_vals_count = df['smoke'].isnull().sum()
    null_percentage = (null_vals_count / len(df['smoke'])) * 100
    print("Number of null values in 'smoke' column after imputation:", null_vals_count, "("+str(null_percentage)+"%)\n")


    print(df[['age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke']].head(20))


    df.to_csv("/tmp/heart_disease_pandas_smoke_imputed.csv", index=False)
    print("Saved pandas data smoke imputed")






# def impute_smoke_func():
#     # 3. Cleaning and imputing the smoke column

#     spark = SparkSession.builder \
#         .appName("Read heart disease dataset") \
#         .getOrCreate()
#     df = spark.read.csv("/tmp/heart_disease_spark_imputed.csv", header=True, inferSchema=True)
    
#     # You will impute the missing values in the “smoke” column with smoking rates
#     #   by age or sex.
#     # You will use two different sources for obtaining these smoking rates.
#     # Create a separate column for each source.
#     # After imputing the missing values, apply an appropriate transform
#     #   for each column
    
    
#     url_1 = "https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking-and-vaping/latest-release"
#     # This source lists smoking rates (current daily smokers) by age group.
#     # Replace the missing values with the smoking rate in the corresponding age groups.
#     response = requests.get(url_1)
#     if response.status_code != 200:
#         print("request failed")
    
#     html_content = response.content
#     full_sel = Selector(text=html_content)
#     table_selector = full_sel.xpath("//table[caption='Proportion of people 15 years and over who were current daily smokers by age, 2011–12 to 2022']/tbody/tr")
    
#     age_to_smoking_rate = {}
#     # Iterate through rows
#     for row in table_selector:
    
#         rowValAgeRange = row.xpath('.//th[@class="row-header"]/text()').extract()[0]
#         if "–" in rowValAgeRange:
#             rangeAges = rowValAgeRange.split("–")
#             ageRange = (int(rangeAges[0]), int(rangeAges[1]))
#         else:
#             ageRange = (int(rowValAgeRange[:2]), float('inf')) # first 2 chars = digits
#         col2022 = row.xpath('.//td[@class="data-value"][position()=last()-2]/text()').extract()[0]
    
#         age_to_smoking_rate[ageRange] = float(col2022)
    
#     print("smoking rates by age (abs):",age_to_smoking_rate)
    
    
#     url_2 = "https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm"
#     # This source lists smoking rates by age group and by sex.
#     # • For female patients, replace the missing values with the smoking rate in
#     #     their corresponding age groups.
#     # • For male patients, replace the missing values with
#     # smoking rate in age group * (smoking rate among men / smoking rate among women)
    
#     response = requests.get(url_2)
#     if response.status_code != 200:
#         print("request failed")
    
#     html_content = response.content
#     full_sel = Selector(text=html_content)
#     row_selector = full_sel.xpath("//div[@class='row '][3]")
    
#     ul_selector = row_selector.xpath("//ul[@class='block-list']")
    
#     sex_li_text = ul_selector[0].xpath(".//li/text()") # sex from first row
#     age_li_text = ul_selector[1].xpath(".//li/text()") # age from second row
    
#     malePercent = float(sex_li_text[0].extract().split("(")[1].split("%)")[0])
#     femalePercent = float(sex_li_text[1].extract().split("(")[1].split("%)")[0])
    
#     ageDict = {}
#     for age_li in age_li_text:
#         ageRange = age_li.extract().split("aged ")[1].split(" years")[0]
#         percentValue = float(age_li.extract().split("(")[1].split("%)")[0])
#         if "–" in ageRange:
#             ages = ageRange.split("–")
#             ageDict[(int(ages[0]), int(ages[1]))] = percentValue
#         else:
#             ageDict[(int(ageRange), float('inf'))] = percentValue
    
#     # ageDict contains the flat rates for each age group. This is what we should
#     #   use to impute for female patients, but we need to change the rates for male
#     #   patients according to the formula:
#     #   smoking rate in age group * (smoking rate among men / smoking rate among women)
    
#     maleSmokingRates = {}
#     for k in ageDict.keys():
#         maleSmokingRates[k] = ageDict[k] * (malePercent / femalePercent)
    
#     print("male smoking rates by age (cdc):",maleSmokingRates)
#     print("female smoking rates by age (cdc):",ageDict)


#     # three cols --> use other two (combination or one of each) to impute the original smoke col
#     # for percents just do random and see if in
    
    
#     # Initialize abs_smoke and cdc_smoke columns
#     df = df.withColumn('abs_smoke', lit(0)).withColumn('cdc_smoke', lit(0))
    
    
    
#     # Apply transformations based on the dictionaries
#     for age_range, percent_chance in age_to_smoking_rate.items():
#         df = df.withColumn(
#             'abs_smoke',
#             when(
#                 (col('age') >= age_range[0]) & (col('age') <= age_range[1]) & (rand() <= percent_chance / 100),
#                 1
#             ).otherwise(col('abs_smoke'))
#         )
    
#     for age_range, percent_chance in maleSmokingRates.items():
#         df = df.withColumn(
#             'cdc_smoke',
#             when(
#                 (col('sex') == 1) & (col('age') >= age_range[0]) & (col('age') <= age_range[1]) & (rand() <= percent_chance / 100),
#                 1
#             ).otherwise(col('cdc_smoke'))
#         )
    
#     for age_range, percent_chance in ageDict.items():
#         df = df.withColumn(
#             'cdc_smoke',
#             when(
#                 (col('sex') == 0) & (col('age') >= age_range[0]) & (col('age') <= age_range[1]) & (rand() <= percent_chance / 100),
#                 1
#             ).otherwise(col('cdc_smoke'))
#         )
    
#     # Show the result
#     print("After creating the abs_smoke and cdc_smoke columns:")
#     df.select('age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke').show(20)
    
#     # Count the number of 1s in abs_smoke
#     num_abs_smoke_ones = df.select(spark_sum(col('abs_smoke')).alias('num_abs_smoke_ones')).collect()[0]['num_abs_smoke_ones']
    
#     # Count the number of 1s in cdc_smoke
#     num_cdc_smoke_ones = df.select(spark_sum(col('cdc_smoke')).alias('num_cdc_smoke_ones')).collect()[0]['num_cdc_smoke_ones']
    
#     print(f"Number of 1s in abs_smoke: {num_abs_smoke_ones}")
#     print(f"Number of 1s in cdc_smoke: {num_cdc_smoke_ones}")


#     # Now impute the smoke column.
    
#     # Calculate the percent for male and female
#     smoke_percent_male = {}
#     smoke_percent_female = {}
#     smoke_starts = {}
#     for k in age_to_smoking_rate.keys():
#         smoke_percent_male[k] = age_to_smoking_rate[k]
#         smoke_percent_female[k] = age_to_smoking_rate[k]
#         smoke_starts[k[0]] = k
    
    
#     for k in maleSmokingRates.keys():
#         rangeStart = k[0]
#         rangeEnd = k[1]
#         if rangeEnd == float('inf'):
#             for k_i in smoke_starts.keys(): # go thru all --> if > start, set it.
#                 if k_i > rangeStart:
#                     # print("setting male", smoke_starts[k_i], "to", (smoke_percent_male[smoke_starts[k_i]] + maleSmokingRates[k]) / 2,
#                     #           "using", smoke_percent_male[smoke_starts[k_i]], "and", maleSmokingRates[k])
#                     smoke_percent_male[smoke_starts[k_i]] = (
#                         smoke_percent_male[smoke_starts[k_i]] + maleSmokingRates[k]
#                     ) / 2
#             break
#         for i in range(rangeStart, rangeEnd):
#             if i in smoke_starts:
#                 # print("setting male", smoke_starts[i], "to", (smoke_percent_male[smoke_starts[i]] + maleSmokingRates[k]) / 2,
#                 #       "using", smoke_percent_male[smoke_starts[i]], "and", maleSmokingRates[k])
#                 smoke_percent_male[smoke_starts[i]] = (
#                     smoke_percent_male[smoke_starts[i]] + maleSmokingRates[k]
#                 ) / 2
    
#     for k in ageDict.keys():
#         rangeStart = k[0]
#         rangeEnd = k[1]
#         if rangeEnd == float('inf'):
#             for k_i in smoke_starts.keys(): # go thru all --> if > start, set it.
#                 if k_i > rangeStart:
#                     # print("setting female", smoke_starts[k_i], "to", (smoke_percent_female[smoke_starts[k_i]] + ageDict[k]) / 2,
#                     #           "using", smoke_percent_female[smoke_starts[k_i]], "and", ageDict[k])
#                     smoke_percent_female[smoke_starts[k_i]] = (
#                         smoke_percent_female[smoke_starts[k_i]] + ageDict[k]
#                     ) / 2
#             break
#         for i in range(rangeStart, rangeEnd):
#             if i in smoke_starts:
#                 # print("setting female", smoke_starts[i], "to", (smoke_percent_female[smoke_starts[i]] + ageDict[k]) / 2,
#                 #       "using", smoke_percent_female[smoke_starts[i]], "and", ageDict[k])
#                 smoke_percent_female[smoke_starts[i]] = (
#                     smoke_percent_female[smoke_starts[i]] + ageDict[k]
#                 ) / 2
    
#     print("Imputing null 'smoke' values with:")
#     print("Age smoke percent male:",smoke_percent_male)
#     print("Age smoke percent female:",smoke_percent_female)

#     # Count the number of null values in the 'smoke' column
#     null_vals_count = df.filter(isnull(col('smoke'))).count()
#     print("Number of null values in 'smoke' column:", null_vals_count)


#     # Function to impute null values in 'smoke' column based on age and sex

    
#     # Function to create a UDF for imputation logic
#     def impute_smoke_udf_func(age, sex):
#         # Get corresponding age smoke percent based on sex
#         age_smoke_percent = smoke_percent_male if sex == 1 else smoke_percent_female
#         # Get random percentage for imputation
#         random_percentage = rd.uniform(0, 100)
#         # If age falls within any range, impute with the corresponding percentage
#         for age_range, percent in age_smoke_percent.items():
#             if age >= age_range[0] and age <= age_range[1]:
#                 return 1 if random_percentage <= percent else 0
#         return None  # Return None if no age range matches

    
    
#     impute_smoke_udf = udf(impute_smoke_udf_func, IntegerType())
    
#     # Create a new column 'imputed_smoke' with imputed values
#     df = df.withColumn("imputed_smoke", 
#                        when(col("smoke").isNull(), impute_smoke_udf(col("age"), col("sex")))
#                        .otherwise(col("smoke")))
    
#     # Drop the old 'smoke' column and rename 'imputed_smoke' to 'smoke'
#     df = df.drop("smoke")
#     df = df.withColumnRenamed("imputed_smoke", "smoke")
    
#     print("Finished imputing values in the 'smoke' column.")
#     df.select('age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke').show(20)


#     # Count the number of null values in the 'smoke' column
#     null_vals_count = df.filter(isnull(col('smoke'))).count()
#     print("Number of null values in 'smoke' column:", null_vals_count)
    
    
#     print("After imputing data:")
#     df.select('age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke').show(20)


#     # Calculate the percentage of null values in each column
#     total_count = df.count()
#     null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
#     null_counts = null_counts.collect()[0].asDict()
    
#     null_percentages = {k: (v / total_count) * 100 for k, v in null_counts.items()}
#     null_percentages_sorted = dict(sorted(null_percentages.items(), key=lambda item: item[1], reverse=True))
    
#     # Print each key-value pair in sorted order
#     print("Percentage of null values in each column:")
#     for column, percentage in null_percentages_sorted.items():
#         print(f"{column}: {percentage:.2f}%")


#     df.write.csv("/tmp/heart_disease_pandas_smoke_imputed.csv", header=True, mode='overwrite')
#     print("Saved Spark data smoke imputed")



def impute(df):
    retain = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 'pro', 'diuretic',
          'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']
    df = df.select([col(c) for c in retain])

    # Calculate the percentage of null values in each column
    total_count = df.count()
    null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    null_counts = null_counts.collect()[0].asDict()
    
    null_percentages = {k: (v / total_count) * 100 for k, v in null_counts.items()}
    null_percentages_sorted = dict(sorted(null_percentages.items(), key=lambda item: item[1], reverse=True))
    
    # Print each key-value pair in sorted order
    print("Percentage of null values in each column:")
    for column, percentage in null_percentages_sorted.items():
        print(f"{column}: {percentage:.2f}%")

    # 2. Cleaning and imputing steps for columns other than `smoke:’
    
    # drop the rows with all columns NULL.
    df = df.dropna(how='all')
    
    
    # painloc, painexer: These are binary columns. Impute by replacing null values with the median.
    binary_cols = ['painloc', 'painexer']
    for col_name in binary_cols:
        median_val = df.approxQuantile(col_name, [0.5], 0.0)[0]
        print("Imputing null", col_name, "rows with median value: " + str(median_val))
        df = df.fillna({col_name: median_val})
    
    # tresbps: Replace values less than 100 mm Hg with null and then replace nulls with the mean.
    df = df.withColumn('trestbps', when(col('trestbps') < 100, None).otherwise(col('trestbps')))
    mean_trestbps = df.filter(df['trestbps'].isNotNull()).select(mean(col('trestbps'))).collect()[0][0]
    print("Imputing null trestbps rows with mean value: " + str(mean_trestbps))
    df = df.fillna({'trestbps': mean_trestbps})
    
    # oldpeak: Replace values less than 0 and those greater than 4 with null, then replace nulls with the mean.
    df = df.withColumn('oldpeak', when((col('oldpeak') < 0) | (col('oldpeak') > 4), None).otherwise(col('oldpeak')))
    mean_oldpeak = df.filter(df['oldpeak'].isNotNull()).select(mean(col('oldpeak'))).collect()[0][0]
    print("Imputing null oldpeak rows with mean value: " + str(mean_oldpeak))
    df = df.fillna({'oldpeak': mean_oldpeak})
    
    # thaldur, thalach: Replace the missing values with the mean.
    continuous_cols = ['thaldur', 'thalach']
    for col_name in continuous_cols:
        mean_val = df.select(mean(col_name)).first()[0]
        print("Imputing null", col_name, "rows with mean value: " + str(mean_val))
        df = df.fillna({col_name: mean_val})
    
    # fbs, prop, nitr, pro, diuretic: Replace the missing values and values greater than 1 with the median.
    binary_cols_with_upper_bound = ['fbs', 'prop', 'nitr', 'pro', 'diuretic']
    for col_name in binary_cols_with_upper_bound:
        df = df.withColumn(col_name, when(col(col_name) > 1, None).otherwise(col(col_name)))
        median_val = df.approxQuantile(col_name, [0.5], 0.0)[0]
        print("Imputing null", col_name, "rows with median value: " + str(median_val))
        df = df.fillna({col_name: median_val})
    
    # exang, slope: Replace the missing values with the median.
    categorical_cols = ['exang', 'slope']
    for col_name in categorical_cols:
        median_val = df.approxQuantile(col_name, [0.5], 0.0)[0]
        print("Imputing null", col_name, "rows with median value: " + str(median_val))
        df = df.fillna({col_name: median_val})
    
    # Show the cleaned DataFrame
    # df.show(5)


    # Drop rows with null values in the 'target' column
    df = df.dropna(subset=['target'])
    
    # Calculate the percentage of null values in each column
    total_count = df.count()
    null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    null_counts = null_counts.collect()[0].asDict()
    
    null_percentages = {k: (v / total_count) * 100 for k, v in null_counts.items()}
    null_percentages_sorted = dict(sorted(null_percentages.items(), key=lambda item: item[1], reverse=True))
    
    # Print each key-value pair in sorted order
    print("Percentage of null values in each column:")
    for column, percentage in null_percentages_sorted.items():
        print(f"{column}: {percentage:.2f}%")

    return df



@from_table_to_df(TABLE_NAMES['original_data'], None)
def clean_data_spark_func(**kwargs):
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Read heart disease dataset") \
        .getOrCreate()

    pandas_df = kwargs['dfs']

    # for column in pandas_df.columns:
    #     if column in ['thaldur', 'oldpeak']:
    #         # Convert specific columns to FloatType
    #         pandas_df[column] = pandas_df[column].astype(float)
    #     else:
    #         pandas_df[column] = pandas_df[column].astype(int)

    print("Pandas df:", pandas_df)
    print(pandas_df.dtypes)

    pandas_df = pandas_df.drop(columns=['pncaden', 'restckm'])

    print("Doing clean/impute spark!")

    df = spark.read.csv("/tmp/heart_disease_brennan.csv", header=True, inferSchema=True)
    df = df.limit(899)

    # Check the schema
    df.printSchema()
    df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

    df = impute(df)
    # df = impute_smoke(df)

    df.write.csv("/tmp/heart_disease_spark_imputed.csv", header=True, mode='overwrite')

    #df.sql_ctx.sparkSession._jsparkSession = spark._jsparkSession
    #df._sc = spark._sc

    return {
        'dfs': [
            {'df': df.toPandas(),
             'table_name': TABLE_NAMES['clean_data_spark']
             }]
        }

    #train(df)


# Spark feature engineering function 1
def spark_feature_eng_1(**kwargs):
    spark = SparkSession.builder \
        .appName("Read heart disease dataset") \
        .getOrCreate()
    data = spark.read.csv("/tmp/heart_disease_spark_imputed.csv", header=True, inferSchema=True)
    data = data.withColumn('thaldur_squared', col('thaldur') ** 2)
    data.write.csv("/tmp/heart_disease_spark_FE1_brennan.csv", header=True, mode='overwrite')
    print("Saved Spark data feature engineering 1")

# Spark feature engineering function 2
def spark_feature_eng_2(**kwargs):
    spark = SparkSession.builder \
        .appName("Read heart disease dataset") \
        .getOrCreate()
    data = spark.read.csv("/tmp/heart_disease_spark_imputed.csv", header=True, inferSchema=True)
    data = data.withColumn('thalach_sqrt', col('thalach') ** 0.5)
    data.write.csv("/tmp/heart_disease_spark_FE2_brennan.csv", header=True, mode='overwrite')
    print("Saved Spark data feature engineering 1")


# Feature engineering pandas
@from_table_to_df(TABLE_NAMES['clean_data_pandas'], None)
def pandas_feature_eng_1(**kwargs):
    df = kwargs['dfs']
    df['thaldur_squared'] = df['thaldur'] ** 2
    df.to_csv("/tmp/heart_disease_pandas_FE1.csv", index=False)
    print("Saved pandas data feature engineering 1")
    return {'dfs': []}

@from_table_to_df(TABLE_NAMES['clean_data_pandas'], None)
def pandas_feature_eng_2(**kwargs):
    df = kwargs['dfs']
    df['thalach_sqrt'] = df['thalach'] ** 0.5
    df.to_csv("/tmp/heart_disease_pandas_FE2.csv", index=False)
    print("Saved pandas data feature engineering 1")
    return {'dfs': []}



def vector_assembler(*args):
    return Vectors.dense(args)


def logistic_train_spark_func():
    """
    train logistic regression on product features
    """

    vector_assembler_udf = udf(vector_assembler, VectorUDT())


    spark = SparkSession.builder \
        .appName("Spark logistic train") \
        .getOrCreate()

    df = spark.read.csv("/tmp/heart_disease_spark_FE1_brennan.csv", header=True, inferSchema=True)

    print("read csv for spark logistic train:", df)

    df = df.drop('smoke')





    # I had trouble making spark machine learning work. 
    # The explanation is at the top of the page.

    df = df.toPandas()



    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of SVC
    model = SklearnLogisticRegression()

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return {"accuracy":accuracy}


    

    nan_counts = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    print("num nans:")
    nan_counts.show()

    features = [f.name for f in df.schema.fields]
    features.remove('target')
    print("at assembler")
    #assembler = VectorAssembler(inputCols=features, outputCol='features')
    features = [f.name for f in df.schema.fields if f.name != 'target']
    df = df.withColumn('features', vector_assembler_udf(*features))
    print("after assembler")
    #df = assembler.transform(df)
    print("after transform")
    

    # Add unique identifier to each row
    df = df.withColumn("id", monotonically_increasing_id())
    
    # stratify dataset on target.
    fractions = df.select("target").distinct().withColumn("fraction", lit(PARAMS['ml']['train_test_ratio'])).rdd.collectAsMap()
    train_set = df.stat.sampleBy("target", fractions, 40)
    
    df_total = df.count()
    
    df_target_1_count = df.filter(col("target") == 1).count()
    df_target_1_percentage = (df_target_1_count / df_total) * 100
    print(f"Percentage of target=1 in the dataset: {df_target_1_percentage:.2f}%")
    
    # Create the test set by excluding the training set
    # test_set = df.subtract(train_set)
    test_set = df.join(train_set, on="id", how="left_anti")
    
    
    train_total = train_set.count()
    test_total = test_set.count()
    # Print the counts of the split sets
    print("Training set count:", train_total, f"({(train_total/df_total * 100):.2f}%)")
    print("Test set count:", test_total, f"({(test_total/df_total * 100):.2f}%)")
    print("Total number of entries:", df_total)
    
    # Show the distribution of the target variable in the training set
    print("\nTrain set:")
    train_set.groupBy("target").count().show()
    train_target_1_count = train_set.filter(col("target") == 1).count()
    train_target_1_percentage = (train_target_1_count / train_total) * 100
    print(f"Percentage of target=1 in the training dataset: {train_target_1_percentage:.2f}%")
    
    # Show the distribution of the target variable in the test set
    print("\nTest set:")
    test_set.groupBy("target").count().show()
    test_target_1_count = test_set.filter(col("target") == 1).count()
    test_target_1_percentage = (test_target_1_count / test_total) * 100
    print(f"Percentage of target=1 in the testing dataset: {test_target_1_percentage:.2f}%")


    # train

    def evaluate_model(predictions):
        evaluator = MulticlassClassificationEvaluator(labelCol="target", predictionCol="prediction", metricName="accuracy")
        return evaluator.evaluate(predictions)
    
    # Logistic regression
    log_reg = LogisticRegressionSpark(labelCol='target', featuresCol='features', maxIter=1000)
    log_reg_model = log_reg.fit(train_set)
    log_reg_predictions = log_reg_model.transform(test_set)
    log_reg_accuracy = evaluate_model(log_reg_predictions)

    print("log reg accuracy spark:", log_reg_accuracy)
    
    # Random forest model
    # rf = RandomForestClassifier(labelCol='target', featuresCol='features')
    # rf_fit = rf.fit(train_set)
    # rf_pred = rf_fit.transform(test_set)
    # rf_accuracy = evaluate_model(rf_pred)

    return {"accuracy":log_reg_accuracy}

def svm_train_spark_func():

    spark = SparkSession.builder \
        .appName("Spark normalize") \
        .getOrCreate()

    # df = spark.createDataFrame(pandas_df, schema=schema)
    df = spark.read.csv("/tmp/heart_disease_spark_FE2_brennan.csv", header=True, inferSchema=True)
    print("read csv for spark svm train:", df)

    df = df.drop('smoke')


    # I had trouble making spark machine learning work. 
    # The explanation is at the top of the page.

    df = df.toPandas()



    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of SVC
    model = SVC(random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return {"accuracy":accuracy}





    nan_counts = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    print("num nans:")
    nan_counts.show()

    features = [f.name for f in df.schema.fields]
    features.remove('target')
    print("at assembler")
    assembler = VectorAssembler(inputCols=features, outputCol='features')
    print("after assembler")
    df = assembler.transform(df)
    print("after transform")
    

    # Add unique identifier to each row
    df = df.withColumn("id", monotonically_increasing_id())
    
    # stratify dataset on target.
    fractions = df.select("target").distinct().withColumn("fraction", lit(PARAMS['ml']['train_test_ratio'])).rdd.collectAsMap()
    train_set = df.stat.sampleBy("target", fractions, 40)
    
    df_total = df.count()
    
    df_target_1_count = df.filter(col("target") == 1).count()
    df_target_1_percentage = (df_target_1_count / df_total) * 100
    print(f"Percentage of target=1 in the dataset: {df_target_1_percentage:.2f}%")
    
    # Create the test set by excluding the training set
    # test_set = df.subtract(train_set)
    test_set = df.join(train_set, on="id", how="left_anti")
    
    
    train_total = train_set.count()
    test_total = test_set.count()
    # Print the counts of the split sets
    print("Training set count:", train_total, f"({(train_total/df_total * 100):.2f}%)")
    print("Test set count:", test_total, f"({(test_total/df_total * 100):.2f}%)")
    print("Total number of entries:", df_total)
    
    # Show the distribution of the target variable in the training set
    print("\nTrain set:")
    train_set.groupBy("target").count().show()
    train_target_1_count = train_set.filter(col("target") == 1).count()
    train_target_1_percentage = (train_target_1_count / train_total) * 100
    print(f"Percentage of target=1 in the training dataset: {train_target_1_percentage:.2f}%")
    
    # Show the distribution of the target variable in the test set
    print("\nTest set:")
    test_set.groupBy("target").count().show()
    test_target_1_count = test_set.filter(col("target") == 1).count()
    test_target_1_percentage = (test_target_1_count / test_total) * 100
    print(f"Percentage of target=1 in the testing dataset: {test_target_1_percentage:.2f}%")


    # train

    def evaluate_model(predictions):
        evaluator = MulticlassClassificationEvaluator(labelCol="target", predictionCol="prediction", metricName="accuracy")
        return evaluator.evaluate(predictions)
    
    # Logistic regression
    svm = LinearSVC(labelCol='target', featuresCol='features', maxIter=70)
    svm_model = svm.fit(train_set)
    svm_predictions = svm_model.transform(test_set)
    svm_accuracy = evaluate_model(svm_predictions)

    print("svm accuracy spark:", svm_accuracy)
    

    return {"accuracy":accuracy}


def logistic_train_pandas_func_smoke():
    """
    train logistic regression on product features
    """


    print("pyspark version:",pyspark.__version__)

    df = pd.read_csv("/tmp/heart_disease_pandas_smoke_imputed.csv")
    print("read csv for spark logistic train:", df)


    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of SVC
    model = SklearnLogisticRegression()

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return {"accuracy":accuracy}





from sklearn.metrics import accuracy_score

def svm_train_pandas_func_smoke():

    df = pd.read_csv("/tmp/heart_disease_pandas_smoke_imputed.csv")
    print("read csv for pandas svm train:", df)


    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of SVC
    model = model = SVC(random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return {"accuracy":accuracy}






#@from_table_to_df([TABLE_NAMES['max_fe'], TABLE_NAMES['train_data_pandas']], TABLE_NAMES["max_fe"])
def logistic_train_pandas_func():
    """
    train logistic regression on max features
    """

    # TODO

    df = pd.read_csv("/tmp/heart_disease_pandas_FE1.csv")
    print("Read csv for logistic train:", df)

    # Split the data into training and validation sets

    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of Logistic Regression model
    model = SklearnLogisticRegression()

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)


    from sklearn.metrics import accuracy_score

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return {"accuracy":accuracy}


def svm_train_pandas_func():

    df = pd.read_csv("/tmp/heart_disease_pandas_FE2.csv")

    print("read csv for svm train:", df)



    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    model = SVC(random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    from sklearn.metrics import accuracy_score

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)    

    return {"accuracy":accuracy}



feature_operations = ["max", "product"] # used when we automatically create tasks
def encode_task_id(feature_operation: str):
    return f'{feature_type}_evaluation'

def decide_which_model(**kwargs):
    """
    perform testing on the best model; if the best model not better than the production model, do nothing
    """
    
    ti = kwargs['ti']
    logistic_pandas = ti.xcom_pull(task_ids='logistic_train_pandas')['accuracy']
    svm_pandas = ti.xcom_pull(task_ids='svm_train_pandas')['accuracy']

    logistic_spark = ti.xcom_pull(task_ids='logistic_train_spark')['accuracy']
    svm_spark = ti.xcom_pull(task_ids='svm_train_spark')['accuracy']

    logistic_pandas_smoke = ti.xcom_pull(task_ids='logistic_train_pandas_smoke')['accuracy']
    svm_pandas_smoke = ti.xcom_pull(task_ids='svm_train_pandas_smoke')['accuracy']
    

    print("logistic_pandas accuracy:", logistic_pandas)
    print("svm_pandas accuracy:", svm_pandas)
    print("logistic_spark accuracy:", logistic_spark)
    print("svm_spark accuracy:", svm_spark)
    print("logistic_pandas_smoke accuracy:", logistic_pandas_smoke)
    print("svm_pandas_smoke accuracy:", svm_pandas_smoke)


    accuracies = {
        'logistic_pandas': logistic_pandas,
        'svm_pandas': svm_pandas,
        'logistic_spark': logistic_spark,
        'svm_spark': svm_spark,
        'logistic_pandas_smoke': logistic_pandas_smoke,
        'svm_pandas_smoke': svm_pandas_smoke
    }

    # Find the variable name with the highest accuracy
    highest_accuracy_var = max(accuracies, key=accuracies.get)

    # Print the variable name of the highest accuracy
    print("The variable with the highest accuracy is:", highest_accuracy_var)


def extensive_evaluation_func(train_df, test_df, fe_type: str, **kwargs):
    """
    train the model on the entire validation data set
    test the final model on test; evaluation also on perturbed test data set
    """
    
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score
    import numpy as np
    from scipy.stats import norm

    model = LogisticRegression()

    # Train the model
    y_train = train_df[PARAMS['ml']['labels']]
    X_train = train_df.drop(PARAMS['ml']['labels'], axis=1)
    model.fit(X_train, y_train)

    def accuracy_on_test(perturb:str = True) -> float:
        X_test = test_df.drop(PARAMS['ml']['labels'], axis=1)
        y_test = test_df[PARAMS['ml']['labels']]

        if perturb == True:
            # we are also perturbing categorical features which is fine since the perturbation is small and thus should not have affect on such features
            X_test = X_test.apply(lambda x: x + np.random.normal(0, PARAMS['ml']['perturbation_std'], len(x)))

        y_pred = model.predict(X_test)
        # Calculate the accuracy of the model
        accuracy = accuracy_score(y_test, y_pred)

        return accuracy

    accuracy = accuracy_on_test(perturb=False)
    print(f"Accuracy on test {accuracy}")

    # we stop when given confidence in accuracy is achieved
    accuracies = []
    for i in range(PARAMS['ml']['max_perturbation_iterations']):
        # Make predictions on the test set
        accuracy = accuracy_on_test()
        accuracies.append(accuracy)

        # compute the confidence interval; break if in the range
        average = np.mean(accuracies)
        std_error = np.std(accuracies) / np.sqrt(len(accuracies))
        confidence_interval = norm.interval(PARAMS['ml']['confidence_level'], loc=average, scale=std_error)
        confidence = confidence_interval[1] - confidence_interval[0]
        if confidence <= 2 * std_error:
            break
    else:
        print(f"Max number of trials reached. Average accuracy on perturbed test {average} with confidence {confidence} and std error of {2 * std_error}")

    print(f"Average accuracy on perturbed test {average}")
    
@from_table_to_df([TABLE_NAMES['train_data_pandas'], TABLE_NAMES['test_data_pandas']], None)
def max_evaluation_func(**kwargs):
    dfs = kwargs['dfs']
    extensive_evaluation_func(dfs[0], dfs[1], "max")

    return {'dfs': []}

@from_table_to_df([TABLE_NAMES['train_data_spark'], TABLE_NAMES['test_data_spark']], None)
def product_evaluation_func(**kwargs):
    dfs = kwargs['dfs']
    extensive_evaluation_func(dfs[0], dfs[1], "product")

    return {'dfs': []}

# Instantiate the DAG
dag = DAG(
    'Brennan_HW4',
    default_args=default_args,
    description='HW4: classify heart disease with feature engineering and model selection',
    schedule_interval=PARAMS['workflow']['workflow_schedule_interval'],
    tags=["de300"]
)

drop_tables = PostgresOperator(
    task_id="drop_tables",
    postgres_conn_id=PARAMS['db']['db_connection'],
    sql=f"""
    DROP SCHEMA public CASCADE;
    CREATE SCHEMA public;
    GRANT ALL ON SCHEMA public TO {PARAMS['db']['username']};
    GRANT ALL ON SCHEMA public TO public;
    COMMENT ON SCHEMA public IS 'standard public schema';
    """,
    dag=dag
)

#download_data = SFTPOperator(
#        task_id="download_data",
#        ssh_hook = SSHHook(ssh_conn_id=PARAMS['files']['sftp_connection']),
#        remote_filepath=PARAMS['files']['remote_file'], 
#        local_filepath=PARAMS['files']['local_file'],
#        operation="get",
#        create_intermediate_dirs=True,
#        dag=dag
#    )

add_data_to_table = PythonOperator(
    task_id='add_data_to_table',
    python_callable=add_data_to_table_func,
    provide_context=True,
    dag=dag
)

clean_data_pandas = PythonOperator(
    task_id='clean_data_pandas',
    python_callable=clean_data_pandas_func,
    provide_context=True,
    dag=dag
)

clean_data_spark = PythonOperator(
    task_id='clean_data_spark',
    python_callable=clean_data_spark_func,
    provide_context=True,
    dag=dag
)

# normalize_data_pandas = PythonOperator(
#     task_id='normalize_data_pandas',
#     python_callable=normalize_data_func_pandas,
#     provide_context=True,
#     dag=dag
# )

# normalize_data_spark = PythonOperator(
#     task_id='normalize_data_spark',
#     python_callable=normalize_data_func_spark,
#     provide_context=True,
#     dag=dag
# )

fe1_spark = PythonOperator(
    task_id='fe1_spark',
    python_callable=spark_feature_eng_1,
    provide_context=True,
    dag=dag,
)

fe2_spark = PythonOperator(
    task_id='fe2_spark',
    python_callable=spark_feature_eng_2,
    provide_context=True,
    dag=dag,
)

fe1_pandas = PythonOperator(
    task_id='fe1_pandas',
    python_callable=pandas_feature_eng_1,
    provide_context=True,
    dag=dag,
)

fe2_pandas = PythonOperator(
    task_id='fe2_pandas',
    python_callable=pandas_feature_eng_2,
    provide_context=True,
    dag=dag,
)


# product_train_spark = PythonOperator(
#     task_id='product_train_spark',
#     python_callable=product_train_spark_func,
#     provide_context=True,
#     dag=dag
# )

# max_train_pandas = PythonOperator(
#     task_id='max_train_pandas',
#     python_callable=max_train_pandas_func,
#     provide_context=True,
#     dag=dag
# )

logistic_train_spark = PythonOperator(
    task_id='logistic_train_spark',
    python_callable=logistic_train_spark_func,
    dag=dag
)

svm_train_spark = PythonOperator(
    task_id='svm_train_spark',
    python_callable=svm_train_spark_func,
    dag=dag
)

logistic_train_pandas = PythonOperator(
    task_id='logistic_train_pandas',
    python_callable=logistic_train_pandas_func,
    provide_context=True,
    dag=dag
)

svm_train_pandas = PythonOperator(
    task_id='svm_train_pandas',
    python_callable=svm_train_pandas_func,
    provide_context=True,
    dag=dag
)

scrape_smoke = PythonOperator(
    task_id='scrape_smoke',
    python_callable=scrape_smoke_func,
    dag=dag
)

impute_smoke_pandas = PythonOperator(
    task_id='impute_smoke_pandas',
    python_callable=impute_smoke_func,
    dag=dag
)

svm_train_pandas_smoke = PythonOperator(
    task_id='svm_train_pandas_smoke',
    python_callable=svm_train_pandas_func_smoke,
    dag=dag
)

logistic_train_pandas_smoke = PythonOperator(
    task_id='logistic_train_pandas_smoke',
    python_callable=logistic_train_pandas_func_smoke,
    dag=dag
)

# production_train = PythonOperator(
#     task_id='production_train',
#     python_callable=production_train_func,
#     provide_context=True,
#     dag=dag
# )

model_selection = BranchPythonOperator(
    task_id='model_selection',
    python_callable=decide_which_model,
    provide_context=True,
    dag=dag,
)

# evaluation_tasks = []
# for feature_type in feature_operations:
#     encoding = encode_task_id(feature_type)
#     evaluation_tasks.append(PythonOperator(
#         task_id=encoding,
#         python_callable=locals()[f'{encoding}_func'],
#         provide_context=True,
#         dag=dag
#     ))

drop_tables >> add_data_to_table 
add_data_to_table >> clean_data_pandas# >> normalize_data_pandas
add_data_to_table >> clean_data_spark# >> normalize_data_spark

clean_data_spark >> [fe1_spark, fe2_spark]
clean_data_pandas >> [fe1_pandas, fe2_pandas]
[clean_data_pandas, scrape_smoke] >> impute_smoke_pandas

fe1_spark >> logistic_train_spark
fe2_spark >> svm_train_spark

fe1_pandas >> logistic_train_pandas
fe2_pandas >> svm_train_pandas

impute_smoke_pandas >> [svm_train_pandas_smoke, logistic_train_pandas_smoke]


[svm_train_pandas_smoke, logistic_train_pandas_smoke, logistic_train_spark, svm_train_spark, logistic_train_pandas, svm_train_pandas] >> model_selection


# normalize_data_pandas >> fe_max_pandas
# normalize_data_spark >> fe_product_spark

# fe_product_spark >> logistic_train_spark
# fe_product_spark >> svm_train_spark

# fe_max_pandas >> logistic_train_pandas
# fe_max_pandas >> svm_train_pandas

# must also put scraping here



# [logistic_train_spark, logistic_train_pandas] >> model_selection
# model_selection >> [dummy_task, *evaluation_tasks]