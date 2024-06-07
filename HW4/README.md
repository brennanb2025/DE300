Here is the DAG MWAA UI: https://0694ff0d-f996-41ea-8ff6-0fa07e68bb3b.c9.us-east-2.airflow.amazonaws.com/dags/Brennan_HW4/grid

My MWAA environment, Robert's, is here: https://us-east-2.console.aws.amazon.com/mwaa/home?region=us-east-2#environments/de300spring2024-airflow-demo
(We were instructed to use his environment.)

Here is my HW4 file: https://us-east-2.console.aws.amazon.com/s3/object/de300spring2024-airflow?region=us-east-2&bucketType=general&prefix=dags/brennan_hw4.py

Postgres RDS database: https://us-east-2.console.aws.amazon.com/rds/home?region=us-east-2#database:id=brennan-database;is-cluster=false

My HW4 file references this config file: https://us-east-2.console.aws.amazon.com/s3/object/de300spring2024-brennanb-airflow?region=us-east-2&bucketType=general&prefix=config_hw4.toml

And this data csv file: https://us-east-2.console.aws.amazon.com/s3/object/de300spring2024-brennanb-airflow?region=us-east-2&bucketType=general&prefix=heart_disease.csv



How the hw4 file works:

A screenshot is included in this folder that shows the DAG.
Click the play button that says "trigger DAG" when hovered over it on my DAG to run it.

Clicking on the Logs of the most recent successful run's model_selection task will show some output.
This is where the machine learning models are trained and evaluated on test data, and it will print the best model at the end.
A screenshot is included for this part too.

Logistic_pandas is sklearn's logistic regression model.
svm_pandas is sklearn's SVM model.
logistic_spark is spark's logistic regression model.
svm_spark is spark's SVM model.
logistic_pandas_smoke is sklearn's logistic regression model with the 'smoke' column imputed with scraped data.
svm_pandas_smoke is sklearn's SVM model with the 'smoke' column imputed with scraped data.

As you can see, it says:
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - logistic_pandas accuracy: 0.7945205479452054
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - svm_pandas accuracy: 0.7054794520547946
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - logistic_spark accuracy: 0.8
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - svm_spark accuracy: 0.6777777777777778
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - logistic_pandas_smoke accuracy: 0.8082191780821918
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - svm_pandas_smoke accuracy: 0.7465753424657534
[2024-06-07, 03:44:31 UTC] {{logging_mixin.py:188}} INFO - The variable with the highest accuracy is: logistic_pandas_smoke