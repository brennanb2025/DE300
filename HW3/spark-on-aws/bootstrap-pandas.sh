#!/bin/bash

python3 pip install --upgrade pip

sudo python3 -m pip uninstall -y urllib3
sudo python3 -m pip install urllib3==1.26.16

sudo python3 -m pip install boto3 numpy scrapy requests "s3fs<=0.4"
                