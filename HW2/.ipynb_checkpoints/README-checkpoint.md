How to run the code:

1. Navigate to the folder "HW2" in my repository.
2. Open a command line.
3. Run the file named "run.sh" (by typing sh run.sh), which will run the jupyter notebook docker container. 
4. Open the Jupyter notebook via the link http://127.0.0.1:8888/tree?token=... in the command line and open the file titled ETL.ipynb.
5. Open the ETL.ipynb file.
6. Go to the nu-sso page to get the access keys: https://nu-sso.awsapps.com/start/#/?tab=accounts
    a. Click the access keys button.
    b. Copy the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN into the code.
    c. Specifically, paste the values into the corresponding locations in the 2nd code block (s3 = boto3.client('s3', ...).
7. Run every cell in the ETL.ipynb file, in order.

Note: you may have to remove existing docker containers if they exist.


I explained my process and justifications in the ETL.ipynb file.


I noticed the syllabus said to specify pseudocode for my algorithms, but because this was just ETL,
I did not use any complicated algorithms.