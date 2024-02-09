# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, year, current_date

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("BigMartSalesDataEngineering").getOrCreate()

# COMMAND ----------

# Read data from CSV file
file_path = 'dbfs:/FileStore/sandhya/mart.csv'  
original_df = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

# Display original data
original_df.show()

# COMMAND ----------

# Data Cleaning
cleaned_df = original_df.withColumn('Item_Weight', when(col('Item_Weight').isNull(), 0).otherwise(col('Item_Weight')))
cleaned_df = cleaned_df.withColumn('Outlet_Size', when(col('Outlet_Size').isNull(), 'Unknown').otherwise(col('Outlet_Size')))

# COMMAND ----------


# Display data after cleaning
cleaned_df.show()

# COMMAND ----------

# Data Transformation
transformed_df = cleaned_df.withColumn('Outlet_Age', year(current_date()) - col('Outlet_Establishment_Year'))
categorical_columns = ['Item_Fat_Content', 'Item_Type', 'Outlet_Identifier', 'Outlet_Size', 'Outlet_Location_Type', 'Outlet_Type']

# COMMAND ----------

for column in categorical_columns:
    transformed_df = transformed_df.withColumn(column + '_Encoded', when(col(column).isNotNull(), col(column)).otherwise('Unknown'))

# COMMAND ----------

# Drop original categorical columns after encoding
transformed_df = transformed_df.drop(*categorical_columns)

# COMMAND ----------

# Display data after transformation
transformed_df.show()

# COMMAND ----------

transformed_df.write.format("delta").mode("overwrite").saveAsTable("kusha_solutions.default.transformeddata")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


