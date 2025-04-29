# 590 DE Project Implementation


## 1. AWS Cloud Setups

The setup is conducted using AWS cloud console. While the exact setup script cannot be retrieved from AWS at this moment, here's a gist of how we setup our instances:

1. AWS EC2 instance with type `t2.medium`.
2. AWS RDS, type is postgresql, we made this database public available and attach it to the same VPC as the EC2. We made it publically available so that it is easier to develop without the complication of the IAM roles. 
3. AWS S3 bucket under the name `590debucket`, allowing public access and allowed public access. This is the single bucket we will be using for all the raw data files.
4. Under the VPC to which both RDS and EC2 are attached, we configure the inbound rules such that port 8000 (for fastapi) and port 8080 (for airflow api) is exposed to all traffic.

## 2. Environment Setup

In your EC2, or any environment you anticipate to run your service, clone the repository.

Deploying this using airflow requires manually installing multiple packages, one can satisfy the environment by installing packages outlined in `requirements.txt` or running `pip install -r requirements.txt`

The configuration files are mostly hard-coded in the code (not the best practice, but we did that for fast development). With the exception that the aws config are stored system wide, and cannot be distributed. One needs to run `aws configure` to set up the aws credentials. 

## 3. Airflow Setup

Follow any guidelines and install the airflow version 3.0.0 (or any other compatible version using the version 2 api). Also, follow official guidelines to setup your aws credentials (if needed) because they will be used by the airflow to access the s3 bucket. 
Prepare the dag files by running the following command:

```bash
cd [path_to_ece_590_final_project]
ln -s [path_to_ece_590_final_project]/DAG ~/airflow/dags 
ln -s [path_to_ece_590_final_project]/my_noaa_api ./DAG/my_noaa_api
```

Run `airflow standalone` to start the airflow instance. Remember the password on its first startup.

## 4. FastAPI + Database Setup

run `python my_noaa_api/app.py` to start the server

If tables are not created, run the app with `--setup-db` flag or set them up manually by executing the sql queries under `RDS_schema_code` directory


## 5. User our endpoints

The documentation can be found here:

https://documenter.getpostman.com/view/24393444/2sB2j1jD2w

Note that for development purposes, although we implementated authentication workflow, but we didn't actually check the api key for this release. You can optionally not provide the API key and will be identified as `anonymous`





