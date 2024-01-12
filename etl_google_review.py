'''
ETL GOOGLE REVIEW
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
import os
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import col, from_unixtime # Se importa explicitamente la funcion col y regexp_replace
spark = SparkSession.builder.master("local[*]").getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab

# Se establecen las rutas de los archivos dentro del bucket
json_files = ["gs://bucket_entrada_pf/review-pennsylvania/1.json", "gs://bucket_entrada_pf/review-pennsylvania/2.json",
              "gs://bucket_entrada_pf/review-pennsylvania/3.json", "gs://bucket_entrada_pf/review-pennsylvania/4.json",
              "gs://bucket_entrada_pf/review-pennsylvania/5.json", "gs://bucket_entrada_pf/review-pennsylvania/6.json",
              "gs://bucket_entrada_pf/review-pennsylvania/7.json", "gs://bucket_entrada_pf/review-pennsylvania/8.json",
              "gs://bucket_entrada_pf/review-pennsylvania/9.json", "gs://bucket_entrada_pf/review-pennsylvania/10.json",
              "gs://bucket_entrada_pf/review-pennsylvania/11.json", "gs://bucket_entrada_pf/review-pennsylvania/12.json",
              "gs://bucket_entrada_pf/review-pennsylvania/13.json", "gs://bucket_entrada_pf/review-pennsylvania/14.json",
              "gs://bucket_entrada_pf/review-pennsylvania/15.json", "gs://bucket_entrada_pf/review-pennsylvania/16.json"]

# Se crea una lista para lamcenar los DatFrames 
dataframes = []

# Se itera sobre los archivos en la ruta
for archivo in (json_files):
      # Se lee el archivo y convertirlo a DataFrame
      df = spark.read.json(archivo)

      # Se agrega el DataFrame a la lista
      dataframes.append(df)
        
# Se combinan todos los dataframes en uno
df_combinado = dataframes[0]  # Se tomar el primer DataFrame como base
for i in range(1, len(dataframes)):
    df_combinado = df_combinado.union(dataframes[i])
    
'''
Transformación del DataFrame
'''

# Se define un nuevo DataFrame donde se extraen los reviews con contenido
df_google_review = df_combinado.filter(col("text").isNotNull())

# Se elimina la columna “pics” del DataFrame
df_google_review = df_google_review.drop("pics")

# Se expande la columna “resp” ya que esta contiene dos campos comprimidos
df_google_review_expanded = df_google_review.select(
    col("gmap_id"),
    col("name"),
    col("rating"),
    col("resp.text").alias("resp_text"),
    col("resp.time").alias("resp_time"),
    col("text"),
    col("time"),
    col("user_id"),
)

# Se verifica que no queden filas duplicadas
df_google_review_expanded = df_google_review_expanded.dropDuplicates()

# Se cambia el tipo de dato de varias columnas y se transforman los números a fechas
df_google_review_expanded = df_google_review_expanded.withColumn("Rating", F.expr("CAST(rating AS INT)")) # Se cambia el tipo de dato a un integer
df_google_review_expanded = df_google_review_expanded.withColumn("Resp_Date", from_unixtime(col("resp_time") / 1000, "yyyy-MM-dd HH:mm:ss")) #Este tipo de dato es un timestamp en milisegundos, se convierte a un Timestamp de pyspark con formato correcto
df_google_review_expanded = df_google_review_expanded.withColumn("Date", from_unixtime(col("time") / 1000, "yyyy-MM-dd HH:mm:ss"))

# Se cambia el tipo de dato de las columnas que almacenan fechas
df_google_review_expanded = df_google_review_expanded.withColumn("Resp_Date", F.to_timestamp(F.col("Resp_Date"))) # TimestampType es el tipo de dato que almacena la fecha y la hora
df_google_review_expanded = df_google_review_expanded.withColumn("Date", F.to_timestamp(F.col("Date"))) 
df_google_review_expanded = df_google_review_expanded.drop("resp_time") # Se elimina las columnas originales con fechas
df_google_review_expanded = df_google_review_expanded.drop("time")

# Se renombran las columnas que faltan
df_google_review_expanded = df_google_review_expanded.withColumnRenamed("gmap_id", "Gmap_Id")
df_google_review_expanded = df_google_review_expanded.withColumnRenamed("name", "Name")
df_google_review_expanded = df_google_review_expanded.withColumnRenamed("resp_text", "Resp_Comment")
df_google_review_expanded = df_google_review_expanded.withColumnRenamed("text", "Comment")
df_google_review_expanded = df_google_review_expanded.withColumnRenamed("user_id", "User_Id")

# Se rellena los valores Nulos del campo "Resp_Comment" con “N/A“
df_google_review_expanded = df_google_review_expanded.na.fill('N/A', subset=['Resp_Comment'])

# Se cambian el orden de las columnas
df_google_review = df_google_review_expanded.select('Gmap_Id','User_Id','Name','Date','Comment','Rating','Resp_Date','Resp_Comment')

'''
Exportar el DataFrame 
'''

# Se extrae el esquema del DataFrame
schema = df_google_review.schema

# Se define la ruta donde se almacenará el archivo procesado 
ruta_guardado = "gs://prometheus_bigquery/google_reviews"

# Se guarda el DataFrame en una ruta especifica del bucket en formato parquet
df_google_review.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema).save(ruta_guardado)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus_sc/ETL_functions/etl_google_review.py