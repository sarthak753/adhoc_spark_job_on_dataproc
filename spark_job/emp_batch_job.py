from pyspark.sql import SparkSession

def process_data():
    
    spark = SparkSession.builder.appName("GCPDataprocJob").config("spark.sql.warehouse.dir","gs://airflow_ass1/hive_data/").enableHiveSupport().getOrCreate()
    # Read bucket name and salary threshold from Spark configs
    bucket = spark.conf.get("spark.bucket.name")
    salary_threshold = int(spark.conf.get("spark.salary.threshold", "50000"))

    # Define paths dynamically
    emp_data_path = f"gs://{bucket}/data/employee.csv"
    dept_data_path = f"gs://{bucket}/data/department.csv"
    output_path = f"gs://{bucket}/output"

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)
    department = spark.read.csv(dept_data_path, header=True, inferSchema=True)

    # Filter employee data
    filtered_employee = employee.filter(employee.salary > salary_threshold)

    # Join datasets
    joined_data = filtered_employee.join(department, "dept_id", "inner")

    # Write output
    #joined_data.write.csv(output_path, mode="overwrite", header=True)

    hive_create_database_query = f"CREATE DATABASE IF NOT EXISTS airflow"
    spark.sql(hive_create_database_query)

    # HQL to create the Hive table inside the 'airflow' database if it doesn't exist
    hive_create_table_query = f"""
        CREATE TABLE IF NOT EXISTS airflow.filtered_employee (
        order_id	INT,
        product	    STRING,
        quantity	INT,
        order_status	STRING,
        order_date	DATE
        )
        STORED AS PARQUET
    """
    # Execute the HQL to create the Hive table
    spark.sql(hive_create_table_query)

    # # Write the filtered employee data to the Hive table in append mode
    joined_data.write.mode("append").format("hive").saveAsTable("airflow.filtered_employee")

    

    spark.stop()

if __name__ == "__main__":
    process_data()
