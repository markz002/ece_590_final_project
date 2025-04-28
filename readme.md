# 590 DE Project Implementation


## 1. AWS Cloud Setups

The setup is conducted using AWS cloud console. While the exact setup script cannot be retrieved from AWS at this moment, here's a gist of how we setup our instances:

1. AWS EC2 instance with type `t2.medium`.
2. AWS RDS, type is postgresql, we made this database public available and attach it to the same VPC as the EC2. We made it publically available so that it is easier to develop without the complication of the IAM roles. 
3. AWS S3 bucket under the name `590debucket`, allowing public access and allowed public access. This is the single bucket we will be using for all the raw data files.
4. Under the VPC to which both RDS and EC2 are attached, we configure the inbound rules such that port 8000 (for fastapi) and port 8080 (for airflow api) is exposed to all traffic.

## 2. Environment Setup

Deploying this using airflow requires manually installing multiple packages, one can satisfy the environment by installing packages outlined in `requirements.txt` or running `pip install -r requirements.txt`
