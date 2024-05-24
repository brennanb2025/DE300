
import boto3
from io import BytesIO


from scrapy import Selector
import requests

import random as rd

import numpy as np


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, count, avg, trim, mean, lit, rand
from pyspark.sql.functions import sum as spark_sum

import os


def read_data(spark: SparkSession) -> DataFrame:

    # local_file_path = os.getcwd() + '/heart_disease.csv'  # Local path where you want to save the file

    # # Download the file from S3
    # try:
    #     s3.download_file(bucket_name, object_key, local_file_path)
    #     print(f"File downloaded successfully to {local_file_path}")
    # except Exception as e:
    #     print(f"Error downloading file: {e}")

    # csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    # body = csv_obj['Body']
    # csv_string = body.read().decode('utf-8')

    # df = pd.read_csv(BytesIO(csv_string.encode()), nrows=899)
    # print("loaded csv into df")
    # print(df.head(20))


    #only retain the following columns:
    # ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 'nitr', 'pro', 'diuretic',
    # 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']

    # Define the schema for the dataset
    schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("sex", IntegerType(), True),
        StructField("painloc", IntegerType(), True),
        StructField("painexer", IntegerType(), True),
        StructField("relrest", IntegerType(), True),
        StructField("pncaden", IntegerType(), True),
        StructField("cp", IntegerType(), True),
        StructField("trestbps", IntegerType(), True),
        StructField("htn", IntegerType(), True),
        StructField("chol", IntegerType(), True),
        StructField("smoke", IntegerType(), True),
        StructField("cigs", IntegerType(), True),
        StructField("years", IntegerType(), True),
        StructField("fbs", IntegerType(), True),
        StructField("dm", IntegerType(), True),
        StructField("famhist", IntegerType(), True),
        StructField("restecg", IntegerType(), True),
        StructField("ekgmo", IntegerType(), True),
        StructField("ekgday(day", IntegerType(), True),
        StructField("ekgyr", IntegerType(), True),
        StructField("dig", IntegerType(), True),
        StructField("prop", IntegerType(), True),
        StructField("nitr", IntegerType(), True),
        StructField("pro", IntegerType(), True),
        StructField("diuretic", IntegerType(), True),
        StructField("proto", IntegerType(), True),
        StructField("thaldur", FloatType(), True),
        StructField("thaltime", IntegerType(), True),
        StructField("met", IntegerType(), True),
        StructField("thalach", IntegerType(), True),
        StructField("thalrest", IntegerType(), True),
        StructField("tpeakbps", IntegerType(), True),
        StructField("tpeakbpd", IntegerType(), True),
        StructField("dummy", IntegerType(), True),
        StructField("trestbpd", IntegerType(), True),
        StructField("exang", IntegerType(), True),
        StructField("xhypo", IntegerType(), True),
        StructField("oldpeak", FloatType(), True),
        StructField("slope", IntegerType(), True),
        StructField("rldv5", IntegerType(), True),
        StructField("rldv5e", IntegerType(), True),
        StructField("ca", IntegerType(), True),
        StructField("restckm", IntegerType(), True),
        StructField("exerckm", IntegerType(), True),
        StructField("restef", IntegerType(), True),
        StructField("restwm", IntegerType(), True),
        StructField("exeref", IntegerType(), True),
        StructField("exerwm", IntegerType(), True),
        StructField("thal", IntegerType(), True),
        StructField("thalsev", IntegerType(), True),
        StructField("thalpul", IntegerType(), True),
        StructField("earlobe", IntegerType(), True),
        StructField("cmo", IntegerType(), True),
        StructField("cday", IntegerType(), True),
        StructField("cyr", IntegerType(), True),
        StructField("target", IntegerType(), True)
    ])

    # Read the dataset
    data = spark.read \
        .schema(schema) \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .csv("s3://brennanbensonawsbucket/data/heart_disease.csv", header = False, inferSchema = False)
        # .csv()

    data = data.limit(900) # limit to first 900 rows

    # Convert columns to appropriate types as per schema
    for field in schema.fields:
        data = data.withColumn(field.name, col(field.name).cast(field.dataType))

    # Show the first 5 rows of the dataset
    data.show(5)

    return data



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
    
    
    from pyspark.sql import functions as F
    
    
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




# Register UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def impute_smoke(df):
    # 3. Cleaning and imputing the smoke column

    
    
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


    # three cols --> use other two (combination or one of each) to impute the original smoke col
    # for percents just do random and see if in
    
    
    # Initialize abs_smoke and cdc_smoke columns
    df = df.withColumn('abs_smoke', lit(0)).withColumn('cdc_smoke', lit(0))
    
    
    
    # Apply transformations based on the dictionaries
    for age_range, percent_chance in age_to_smoking_rate.items():
        df = df.withColumn(
            'abs_smoke',
            when(
                (col('age') >= age_range[0]) & (col('age') <= age_range[1]) & (rand() <= percent_chance / 100),
                1
            ).otherwise(col('abs_smoke'))
        )
    
    for age_range, percent_chance in maleSmokingRates.items():
        df = df.withColumn(
            'cdc_smoke',
            when(
                (col('sex') == 1) & (col('age') >= age_range[0]) & (col('age') <= age_range[1]) & (rand() <= percent_chance / 100),
                1
            ).otherwise(col('cdc_smoke'))
        )
    
    for age_range, percent_chance in ageDict.items():
        df = df.withColumn(
            'cdc_smoke',
            when(
                (col('sex') == 0) & (col('age') >= age_range[0]) & (col('age') <= age_range[1]) & (rand() <= percent_chance / 100),
                1
            ).otherwise(col('cdc_smoke'))
        )
    
    # Show the result
    print("After creating the abs_smoke and cdc_smoke columns:")
    df.select('age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke').show(20)
    
    # Count the number of 1s in abs_smoke
    num_abs_smoke_ones = df.select(spark_sum(col('abs_smoke')).alias('num_abs_smoke_ones')).collect()[0]['num_abs_smoke_ones']
    
    # Count the number of 1s in cdc_smoke
    num_cdc_smoke_ones = df.select(spark_sum(col('cdc_smoke')).alias('num_cdc_smoke_ones')).collect()[0]['num_cdc_smoke_ones']
    
    print(f"Number of 1s in abs_smoke: {num_abs_smoke_ones}")
    print(f"Number of 1s in cdc_smoke: {num_cdc_smoke_ones}")


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

    # Count the number of null values in the 'smoke' column
    null_vals_count = df.filter(isnull(col('smoke'))).count()
    print("Number of null values in 'smoke' column:", null_vals_count)


    # Function to impute null values in 'smoke' column based on age and sex

    
    # Function to create a UDF for imputation logic
    def impute_smoke_udf_func(age, sex):
        # Get corresponding age smoke percent based on sex
        age_smoke_percent = smoke_percent_male if sex == 1 else smoke_percent_female
        # Get random percentage for imputation
        random_percentage = rd.uniform(0, 100)
        # If age falls within any range, impute with the corresponding percentage
        for age_range, percent in age_smoke_percent.items():
            if age >= age_range[0] and age <= age_range[1]:
                return 1 if random_percentage <= percent else 0
        return None  # Return None if no age range matches

    
    
    impute_smoke_udf = udf(impute_smoke_udf_func, IntegerType())
    
    # Create a new column 'imputed_smoke' with imputed values
    df = df.withColumn("imputed_smoke", 
                       when(col("smoke").isNull(), impute_smoke_udf(col("age"), col("sex")))
                       .otherwise(col("smoke")))
    
    # Drop the old 'smoke' column and rename 'imputed_smoke' to 'smoke'
    df = df.drop("smoke")
    df = df.withColumnRenamed("imputed_smoke", "smoke")
    
    print("Finished imputing values in the 'smoke' column.")
    df.select('age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke').show(20)


    # Count the number of null values in the 'smoke' column
    null_vals_count = df.filter(isnull(col('smoke'))).count()
    print("Number of null values in 'smoke' column:", null_vals_count)
    
    
    print("After imputing data:")
    df.select('age', 'sex', 'smoke', 'abs_smoke', 'cdc_smoke').show(20)


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


from pyspark.sql.functions import col, lit, monotonically_increasing_id
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, LinearSVC
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.sql.window import Window
from pyspark.mllib.evaluation import MulticlassMetrics


def train(df):

    features = [f.name for f in df.schema.fields]
    features.remove('target')
    # Assemble feature columns into a single feature vector
    assembler = VectorAssembler(
        inputCols=features, 
        outputCol="features"
        )
    df = assembler.transform(df)

    # # Define a Random Forest classifier
    # classifier = RandomForestClassifier(labelCol="target", featuresCol="features")

    # # Create the pipeline
    # pipeline = Pipeline(stages=[assembler, classifier])

    # # Set up the parameter grid for maximum tree depth
    # paramGrid = ParamGridBuilder() \
    #     .addGrid(classifier.maxDepth, [2, 4, 6, 8, 10]) \
    #     .build()

    # # Set up the cross-validator
    # evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    # crossval = CrossValidator(
    #     estimator=pipeline,
    #     estimatorParamMaps=paramGrid,
    #     evaluator=evaluator,
    #     numFolds=3,
    #     seed=3004)
    

    # Training

    # from sklearn.model_selection import train_test_split
    
    # Split the data into training and test using a 90-10 training test split with stratification on
    # the labels (i.e., both sets contain roughly the same proportion of positive labels).
    
    
    # Define features (X) and target variable (y)
    # X = df.drop(columns=['target'])  # Features (all except target)
    # y = df['target']  # Target variable
    
    # # Split the data into training and test sets with stratification
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, stratify=y, random_state=42)
    
    # # Check the shapes of the split sets
    # print("Training set shape:", X_train.shape, y_train.shape)
    # print("Test set shape:", X_test.shape, y_test.shape)
    


    # split and stratify data

    
    # Add unique identifier to each row
    df = df.withColumn("id", monotonically_increasing_id())
    
    # stratify dataset on target.
    fractions = df.select("target").distinct().withColumn("fraction", lit(0.9)).rdd.collectAsMap()
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
    log_reg = LogisticRegression(labelCol='target', featuresCol='features')
    log_reg_model = log_reg.fit(train_set)
    log_reg_predictions = log_reg_model.transform(test_set)
    log_reg_accuracy = evaluate_model(log_reg_predictions)
    
    # Random forest model
    rf = RandomForestClassifier(labelCol='target', featuresCol='features')
    rf_fit = rf.fit(train_set)
    rf_pred = rf_fit.transform(test_set)
    rf_accuracy = evaluate_model(rf_pred)

    if rf_accuracy > log_reg_accuracy:
        print(f"The best model is random forest with an accuracy of {rf_accuracy:.2f}")
    else:
        print(f"The best model is logistic regression with an accuracy of {log_reg_accuracy:.2f}")



import os

def save_data(df):
    # Save the dataframe back to the s3 bucket as a CSV file.

    # you need to change the credentials for yourself
    
    bucket_name = 'brennanbensonawsbucket'
    object_key = 'data/heart_disease/'
    
    s3 = boto3.client('s3',
                      aws_access_key_id=,
                      aws_secret_access_key=,
                      aws_session_token=
                      )
    
    # Save the DataFrame as a CSV file
    df.repartition(1).write.csv("heart_disease_cleaned", mode="overwrite")
    
    
    # Get the current directory
    current_directory = os.getcwd() + "/heart_disease_cleaned"
    
    # List all files in the current directory
    files = os.listdir(current_directory)
    
    # Filter CSV files
    csv_files = [file for file in files if file.endswith('.csv')]
    
    # Upload each CSV file to S3
    for csv_file in csv_files:
        local_file_path = os.path.join(current_directory, csv_file)
        object_key = f'data/heart_disease_cleaned/{csv_file}'  # Change this to your desired S3 directory structure
        try:
            s3.upload_file(local_file_path, bucket_name, object_key)
            print(f"File '{csv_file}' uploaded successfully to s3://{bucket_name}/{object_key}")
        except Exception as e:
            print(f"Error uploading file '{csv_file}' to S3: {e}")
        
        
        

####################################### main #######################################
def main():
    # Create a Spark session
    spark = SparkSession.builder \
    .appName("Read heart disease dataset") \
    .getOrCreate()

    df = read_data(spark)

    # Check the schema
    df.printSchema()
    df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

    df = impute(df)
    df = impute_smoke(df)

    train(df)

    # save_data(df)
    # no need to save cleaned data - will be the same as last lab.
    # a previous run of the cleaned data from a local run is in the following S3 folder: 
    # s3://brennanbensonawsbucket/data/heart_disease_cleaned/

    spark.stop()
    
main()