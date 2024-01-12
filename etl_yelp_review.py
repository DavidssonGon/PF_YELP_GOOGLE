'''
ETL YELP REVIEW
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import col, when, regexp_extract # Se importa explicitamente algunas funciones
spark = SparkSession.builder.master("local[*]").getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab

# Se establece la ruta del archivo dentro del bucket
ruta = "gs://bucket_entrada_pf/review.json"

# Se lee el archivo con extensión .json con spark
df_review = spark.read.json(ruta)

# Se cambia el tipo de dato de varias columnas y se normalizan sus nombres
df_review = df_review.withColumn("Date", F.to_timestamp(F.col("date"))) # TimestampType es el tipo de dato que almacena la fecha y la hora
df_review = df_review.withColumn("Cool",  F.expr("CAST(cool AS INT)"))
df_review = df_review.withColumn("Funny",  F.expr("CAST(funny AS INT)"))
df_review = df_review.withColumn("Useful",  F.expr("CAST(useful AS INT)"))

# Se renombran las columnas que faltan
df_review = df_review.withColumnRenamed("business_id", "Business_Id")
df_review = df_review.withColumnRenamed("user_id", "User_Id")
df_review = df_review.withColumnRenamed("text", "Comment")
df_review = df_review.withColumnRenamed("stars", "Stars")

# Se aplica el cambio en el valor de la columna "Cool" de "-1" a "0"
df_review = df_review.withColumn("Cool", when(col("Cool") == -1, 0).otherwise(col("Cool")))

# Se aplica el cambio en el valor de la columna "Funny" de "-1" a "0"
df_review = df_review.withColumn("Funny", when(col("Funny") == -1, 0).otherwise(col("Funny")))

# Se aplica el cambio en el valor de la columna "Useful" de "-1" a "0"
df_review = df_review.withColumn("Useful", when(col("Useful") == -1, 0).otherwise(col("Useful")))

'''
En este punto se aplica una transformación al DataFrame para eliminar los comentarios con caracteres no latinos.
'''

# Se define una expresión regular que incluirá los caracteres usados en los idiomas occidentales
expresion_regular = "^[a-zA-Z0-9.,!?';*()$&#=+_-¡¿:\s]+$"

# Se redefine el DataFrame aplicando un filtro donde se usa la función `regexp_extract` en el campo “Coment” donde 
# las cadenas que sí contengan cualquier carácter definido en la expresión regular son las que quedan en el DataFrame
df_review = df_review.filter(regexp_extract(col("Comment"), expresion_regular, 0) != "")

# Se redefine el DataFrame ordenando las filas a partir del campo “Date” de la fecha más antigua a la mas reciente
df_review = df_review.sort(F.col("Date").asc())

# Se cambian el orden de las columnas
columnas_ordenas = ['Business_Id','User_Id','Date','Comment','Cool','Funny','Useful','Stars']
df_review = df_review.select(columnas_ordenas)

'''
Exportar el DataFrame 
'''

# Se extrae el esquema del DataFrame
schema = df_review.schema

# Se define la ruta donde se almacenará el archivo procesado 
ruta_guardado = "gs://prometheus_bigquery/yelp_reviews"

# Se guarda el DataFrame en una ruta especifica del bucket en formato parquet
df_review.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema).save(ruta_guardado)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus_sc/ETL_functions/etl_yelp_review.py