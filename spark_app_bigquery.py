#import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import date
from pyspark.sql.types import *



bucket = f"airflow_gcs_resume_bucket"
print(date.today())
data_path = f"gs://{bucket}/input_data/health_data_{date.today()}.csv"
print(data_path)
output_path =f"gs://{bucket}/output_data/"




spark = SparkSession.builder.appName("Health-Analysis").getOrCreate()
hdata =spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data_path)

# bucket_temp = "[airflow_gcs_resume_bucket/stg]"
# spark.conf.set('temporaryGcsBucket', bucket_temp)

    # Filter employee data
filtered_data = hdata.filter(col("age")<60)
data = filtered_data.select(col("patient_id"),col("age"),col("gender"),col("diagnosis_code"),col("diagnosis_description"),col("diagnosis_date").cast(StringType()).alias("diagnosis_date"))
data.show(truncate=False)

    # Write output
data.write.csv(output_path, header=True)
data.write.mode('append').format('bigquery').option("temporaryGcsBucket","airflow_gcs_resume_bucket_temp").save('pyspark-learning-407410.Healthcare.health_data')



# #Function to create a dataset in Bigquery
# def bq_create_dataset(client, dataset):
#     dataset_ref = bigquery_client.dataset(dataset)

#     try:
#         dataset = bigquery_client.get_dataset(dataset_ref)
#         print('Dataset {} already exists.'.format(dataset))
#     except NotFound:
#         dataset = bigquery.Dataset(dataset_ref)
#         dataset.location = 'US'
#         dataset = bigquery_client.create_dataset(dataset)
#         print('Dataset {} created.'.format(dataset.dataset_id))
#     return dataset

# def bq_create_table(client, dataset, table_name):
#     dataset_ref = bigquery_client.dataset(dataset)

#     # Prepares a reference to the table
#     table_ref = dataset_ref.table(table_name)

#     try:
#         table =  bigquery_client.get_table(table_ref)
#         print('table {} already exists.'.format(table))
#     except NotFound:
#         schema = [
#             bigquery.SchemaField("patient_id", "STRING", mode="REQUIRED"),
#             bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
#             bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
#             bigquery.SchemaField("diagnosis_code", "STRING", mode="REQUIRED"),
#             bigquery.SchemaField("diagnosis_description", "STRING", mode="REQUIRED"),
#             bigquery.SchemaField("diagnosis_date", "STRING", mode="REQUIRED")
#         ]
#         table = bigquery.Table(table_ref, schema=schema)
#         table = bigquery_client.create_table(table)
#         print('table {} created.'.format(table.table_id))
#     return table

# def export_items_to_bigquery(client, dataset, table):    

#     # Prepares a reference to the dataset
#     dataset_ref = bigquery_client.dataset(dataset)

#     table_ref = dataset_ref.table(table)
#     table = bigquery_client.get_table(table_ref)  # API call
    
    
#     errors = bigquery_client.insert_rows(table, filtered_data)  # API request
#     assert errors == []
    

# if __name__ == "__main__":
#     #creating bigquery object
#     bigquery_client = bigquery.Client()
#     dataset = "Healthcare"
#     table_name = "health_data"
#     data = bq_create_dataset(bigquery_client, dataset)
#     table = bq_create_table(bigquery_client, dataset, table_name)
#     export_items_to_bigquery(bigquery_client, dataset, table_name)    
spark.stop()