from pyspark.sql import SparkSession
from airflow.models import Variable

def process_data():
    spark = SparkSession.builder.appName("GCPDataprocJob").getOrCreate()

    # Define your GCS bucket and paths from Airflow Variables
    bucket = Variable.get("bucket")
    emp_data_path = f"gs://{bucket}/data/employee.csv"
    dept_data_path = f"gs://{bucket}/data/department.csv"
    output_path = f"gs://{bucket}/output"

    # Salary threshold from Airflow Variables
    salary_threshold = int(Variable.get("salary_threshold"))

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)
    department = spark.read.csv(dept_data_path, header=True, inferSchema=True)

    # Filter employee data
    filtered_employee = employee.filter(employee.salary > salary_threshold)

    # Join datasets
    joined_data = filtered_employee.join(department, "dept_id", "inner")

    # Write output
    joined_data.write.csv(output_path, mode="overwrite", header=True)

    spark.stop()

if __name__ == "__main__":
    process_data()
