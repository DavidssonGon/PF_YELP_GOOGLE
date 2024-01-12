'''
ETL YELP USER
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import lit, col, split, explode, regexp_replace, round # Se importa explicitamente la funcion col y regexp_replace
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("example") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab
    
# Se establece la ruta del archivo dentro del bucket
ruta = "gs://bucket_entrada_pf/user.parquet"

# Se lee el archivo con extensión .parquet con spark
df_user = spark.read.parquet(ruta).repartition(8)

'''
Transformación del DataFrame
'''

# Se eliminan duplicados
df_user = df_user.dropDuplicates() # Se redefine el DataFrame aplicandole un `dropDuplicates()` para eliminar las filas duplicadas

# Se cambia el tipo de dato de las columnas y se normalizan sus nombres
df_user = df_user.withColumn("Yelping_Since", F.to_timestamp(F.col("yelping_since"))) # TimestampType es el tipo de dato que almacena la fecha y la hora
df_user = df_user.withColumn("Review_Count",  F.expr("CAST(review_count AS INT)")) 
df_user = df_user.withColumn("Useful",  F.expr("CAST(useful AS INT)")) 
df_user = df_user.withColumn("Funny",  F.expr("CAST(funny AS INT)")) 
df_user = df_user.withColumn("Cool",  F.expr("CAST(cool AS INT)")) 
df_user = df_user.withColumn("Fans",  F.expr("CAST(fans AS INT)")) 
df_user = df_user.withColumn("Average_Stars", round(col("average_stars").cast("decimal(2,1)"), 1))
df_user = df_user.withColumn("Compliment_Hot",  F.expr("CAST(compliment_hot AS INT)"))
df_user = df_user.withColumn("Compliment_More",  F.expr("CAST(compliment_more AS INT)"))
df_user = df_user.withColumn("Compliment_Profile",  F.expr("CAST(compliment_profile AS INT)"))
df_user = df_user.withColumn("Compliment_Cute",  F.expr("CAST(compliment_cute AS INT)"))
df_user = df_user.withColumn("Compliment_List",  F.expr("CAST(compliment_list AS INT)"))
df_user = df_user.withColumn("Compliment_Note",  F.expr("CAST(compliment_note AS INT)"))
df_user = df_user.withColumn("Compliment_Plain",  F.expr("CAST(compliment_plain AS INT)"))
df_user = df_user.withColumn("Compliment_Cool",  F.expr("CAST(compliment_cool AS INT)"))
df_user = df_user.withColumn("Compliment_Funny",  F.expr("CAST(compliment_funny AS INT)"))
df_user = df_user.withColumn("Compliment_Writer",  F.expr("CAST(compliment_writer AS INT)"))
df_user = df_user.withColumn("Compliment_Photos",  F.expr("CAST(compliment_photos AS INT)"))

# Se renombran las columnas que faltan
df_user = df_user.withColumnRenamed("user_id", "User_Id")
df_user = df_user.withColumnRenamed("name", "Name")
df_user = df_user.withColumnRenamed("elite", "Elite")
df_user = df_user.withColumnRenamed("friends", "Friends")
df_user = df_user.withColumnRenamed("friends", "Friends")

# Se cambian el orden de las columnas
columnas_ordenas = ['User_Id','Name','Yelping_Since','Elite', 'Review_Count', 'Average_Stars','Useful', 'Funny', 'Cool', 'Fans', 'Compliment_Hot', 'Compliment_More', 'Compliment_Profile', 'Compliment_Cute', 'Compliment_List', 'Compliment_Note', 'Compliment_Plain', 'Compliment_Cool', 'Compliment_Funny', 'Compliment_Writer', 'Compliment_Photos', 'Friends']
df_user = df_user.select(columnas_ordenas)    

# Se redefine el DataFrame ordenando las filas a partir del campo “Yelping_Since” de la fecha más antigua a la mas reciente
df_user = df_user.sort(F.col("Yelping_Since").asc())

'''
Exportar el DataFrame 
'''

# Se guarda el esquema del DataFrame
schema = df_user.schema

# Se define la ruta donde se almacenará el archivo procesado 
ruta_guardado = "gs://prometheus_bigquery/yelp_user"

# Se guarda el DataFrame en una ruta especifica del bucket en formato parquet
df_user.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema).save(ruta_guardado)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus_sc/ETL_functions/etl_yelp_user.py