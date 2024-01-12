'''
ETL YELP CHECKIN
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import col, split, explode # Se importa explicitamente la funcion col y regexp_replace
spark = SparkSession.builder.master("local[*]").getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab

# Se establece la ruta del archivo dentro del bucket
ruta = "gs://bucket_entrada_pf/checkin.json"

# Se lee el archivo con extensión .json con spark
df_checkin = spark.read.json(ruta)

    
'''
Transformación del DataFrame
'''

# Se convierte cada fecha separada por una coma en una lista
df_split = df_checkin.withColumn("date", split(df_checkin["date"], ", "))

# Se utiliza explode para convertir las listas en registros individuales
df_checkin_explotado = df_split.select("business_id", explode("date").alias("date"))

# Se cambia el tipo de dato de la columna date y se normaliza su nombre
df_checkin_explotado = df_checkin_explotado.withColumn("Date", F.to_timestamp(F.col("date"))) # TimestampType es el tipo de dato que almacena la fecha y la hora

# Se renombra la columna que falta
df_checkin_explotado = df_checkin_explotado.withColumnRenamed("business_id", "Business_Id")

# Se redefine el DataFrame ordenando las filas a partir del campo “Date” de la fecha más antigua a la mas reciente
df_checkin = df_checkin_explotado.sort(F.col("Date").asc())

'''
Exportar el DataFrame 
'''

# Se guarda el esquema del DataFrame
schema = df_checkin.schema

# Se define la ruta donde se almacenará el archivo procesado 
ruta_guardado = "gs://prometheus_bigquery/yelp_checkin"

# Se guarda el DataFrame en una ruta especifica del bucket en formato parquet
df_checkin.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema).save(ruta_guardado)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus_sc/ETL_functions/etl_yelp_checkin.py