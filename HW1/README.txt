How to run the code:

1. Navigate to the folder "HW1" in my repository.
2. Open a command line.
3. Run: docker pull mysql:latest
4. Run: docker network create my-network
5. Run: docker run -d --name my-mysql \
    -e MYSQL_ROOT_PASSWORD=root_pwd \
    -e MYSQL_DATABASE=DE300db \
    --network my-network \
    mysql
6. Run: docker build -t eda .
This took about 3 minutes for me.
7. Run: docker run -p 8888:8888 --network my-network eda
8. Open the Jupyter notebook via the link http://127.0.0.1:8888/tree?token=... to run and see the python file titled EDA.ipynb.
9. Open the EDA.ipynb file.
10. Run every cell in the EDA.ipynb file, in order.

Note: you may have to remove existing docker containers if they exist.


I explained my process in the file, but I'll run through my EDA steps again here.


I noticed the syllabus said to specify pseudocode for my algorithms, but because this was just EDA,
I did not use any complicated algorithms.