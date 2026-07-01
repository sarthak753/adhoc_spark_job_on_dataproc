# Airflow Dataproc Serverless Workflow

This project demonstrates how to run a **PySpark job on Google Cloud Dataproc Serverless (Batch)** using Apache Airflow.  
The DAG checks for input files in GCS, submits a serverless PySpark job, and writes results back to GCS.

---

## ⚙️ Workflow Steps
1. **File Sensor** → Check if input CSV files exist in GCS.  
2. **Dataproc Serverless Batch** → Submit PySpark job (`emp_batch_job.py`).  
3. **PySpark Processing** → Filter employees by salary, join
