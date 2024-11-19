Created Spark DAG pipeline, hosted in AWS MWAA using Apache Airflow, to:
1. Read configuration file from S3
2. Use Python Pandas to load heart disease data from .csv file and use scrapy to scrape data from web to impute missing values in 'smoke' column based on age
3. Add data to RDS database for further imputation/engineering
4. Impute data to remove outliers
5. Use Spark and Pandas to perform feature engineering on specific columns
6. Train Spark and SKLearn SVC/logistic regression models with data
7. Evaluate each model's performance and choose best model

Full code is [here](https://github.com/brennanb2025/DE300/tree/main/HW4).

Screenshots of the pipeline:
<img width="1495" alt="Screenshot 2024-06-07 at 4 33 35 PM" src="https://github.com/user-attachments/assets/39ba5f42-be11-4509-af17-e2c30ba19b69">
<img width="873" alt="Screenshot 2024-06-07 at 4 32 30 PM" src="https://github.com/user-attachments/assets/da01f3b1-4321-499a-99ec-21e9078572fc">
