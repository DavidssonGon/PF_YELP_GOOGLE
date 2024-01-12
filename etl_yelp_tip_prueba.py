'''
ETL YELP TIP
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import col, regexp_extract  # Se importa explicitamente la funcion col y regexp_replace
spark = SparkSession.builder.master("local[*]").getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab

# Se establece la ruta del archivo dentro del bucket
ruta = "gs://bucket_entrada_pf/tip.json"

# Se lee el archivo con extensión .json con spark
df_tip = spark.read.json(ruta)

'''
Transformación del DataFrame
'''

#Se eliminan filas duplicadas
df_tip = df_tip.dropDuplicates()

# Se cambia el tipo de dato de la columna y se normaliza su nombre
df_tip = df_tip.withColumn("Date", F.to_timestamp(F.col("date"))) # TimestampType es el tipo de dato que almacena la fecha y la hora
df_tip = df_tip.withColumn("Compliment_Count",  F.expr("CAST(compliment_count AS INT)"))

# Se renombran las columnas que faltan
df_tip = df_tip.withColumnRenamed("business_id", "Business_Id")
df_tip = df_tip.withColumnRenamed("text", "Comment")
df_tip = df_tip.withColumnRenamed("user_id", "User_Id")

# Se define una expresión regular que incluirá los caracteres usados en los idiomas occidentales
expresion_regular = "^[a-zA-Z0-9.,!?';*()$&#=+_-¡¿:\s]+$"

'''Se redefine el DataFrame aplicando un filtro donde se usa la función `regexp_extract` en el campo “Comment” 
donde las cadenas que sí contengan cualquier carácter definido en la expresión regular son las que quedan 
en el DataFrame'''
df_tip = df_tip.filter(regexp_extract(col("Comment"), expresion_regular, 0) != "")

# Se redefine el DataFrame ordenando las filas a partir del campo “Date” de la fecha más antigua a la mas reciente
df_tip = df_tip.sort(F.col("Date").asc())

# Se cambian el orden de las columnas
columnas_ordenas = ['Business_Id','User_Id','Date','Comment','Compliment_Count']
df_tip = df_tip.select(columnas_ordenas)

'''
Exportar el DataFrame 
'''

# Se guarda el esquema del DataFrame
schema = df_tip.schema

# Se define la ruta donde se almacenará el archivo procesado 
ruta_guardado = "gs://prometheus_bigquery/yelp_tip"

# Se guarda el DataFrame en una ruta especifica del bucket en formato parquet
df_tip.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema).save(ruta_guardado)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus/ETL_functions/etl_yelp_tip.py