from pyspark.sql import SparkSession

def process_data():
    # Initialize Spark session (no Hive support for serverless)
    spark = SparkSession.builder.appName("GCPDataprocJob").getOrCreate()

    # Read bucket name and salary threshold from Spark configs
    bucket = spark.conf.get("spark.bucket.name")
    salary_threshold = int(spark.conf.get("spark.salary.threshold", "50000"))

    # Define paths dynamically
    emp_data_path = f"gs://{bucket}/data/employee.csv"
    dept_data_path = f"gs://{bucket}/data/department.csv"
    output_path = f"gs://{bucket}/output/filtered_employee"

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)
    department = spark.read.csv(dept_data_path, header=True, inferSchema=True)

    # Ensure salary is integer
    employee = employee.withColumn("salary", employee["salary"].cast("int"))

    # Filter employee data
    filtered_employee = employee.filter(employee.salary > salary_threshold)

    # Join datasets
    joined_data = filtered_employee.join(department, "dept_id", "inner")

    # Write output to GCS in Parquet format
    joined_data.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    process_data()
