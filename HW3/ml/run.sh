docker run -v ~/DE300repo/DE300/HW3/ml:/tmp/ml -it \
           -p 8888:8888 \
           --name spark-sql-container \
	   pyspark-image