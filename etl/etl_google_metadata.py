'''
ETL GOOGLE METADATA
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import lit, col, concat_ws, expr # Se importa explicitamente la funcion col y regexp_replace
spark = SparkSession.builder.master("local[*]").getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab

'''
Primer DataFrame
'''

# Se establece la ruta del archivo dentro del bucket
json_files_df_1 = ["gs://bucket_entrada_pf/metadata-sitios/1.json", "gs://bucket_entrada_pf/metadata-sitios/2.json",
                   "gs://bucket_entrada_pf/metadata-sitios/3.json", "gs://bucket_entrada_pf/metadata-sitios/6.json",
                   "gs://bucket_entrada_pf/metadata-sitios/9.json"]

# Se crea una lista para lamcenar los DataFrames
dataframes_df_1 = []

# Se lee los archivos para convertirlos a DataFrame
for archivo in (json_files_df_1):
      df = spark.read.json(archivo)

      # Se agrega el DataFrame a la lista
      dataframes_df_1.append(df)
      
# Se combinan todos los dataframes en uno
df_combinado_1 = dataframes_df_1[0]  # Se tomar el primer DataFrame como base
for i in range(1, len(dataframes_df_1)):
    df_combinado_1 = df_combinado_1.union(dataframes_df_1[i])

'''
Transformación individual del DataFrame
'''

# Se añade una nueva columna que convierta la columna "hours" a un formato de string
df_expandido_1 = df_combinado_1.withColumn(
    "hours_str",
    concat_ws(", ", expr("transform(hours, x -> concat_ws(', ', x))")))

# Se expanden las columnas del DataFrame al tiempo que se filtra campos relevantes
df_expandido_1 = df_expandido_1.select(
    col("address"),
    col("avg_rating"),
    col("description"),
    col("gmap_id"),
    col("latitude"),
    col("longitude"),
    col("name"),
    col("num_of_reviews"),
    col("state"),
    col("hours_str").alias("Hours"),
    # Columnas sin la estructura MISC
    concat_ws(", ", col("category").alias("Category")),
    concat_ws(", ", col("relative_results").alias("Relative_Results")),
    # Columnas de la estructura MISC
    concat_ws(", ", col("MISC.Accessibility").getItem(0).alias("Accessibility")),
    concat_ws(", ", col("MISC.Activities").getItem(0).alias("Activities")),
    concat_ws(", ", col("MISC.Amenities").getItem(0).alias("Amenities")),
    concat_ws(", ", col("MISC.Atmosphere").getItem(0).alias("Atmosphere")),
    concat_ws(", ", col("MISC.Crowd").getItem(0).alias("Crowd")),
    concat_ws(", ", col("MISC.Dining options").getItem(0).alias("Dining_options")),
    concat_ws(", ", col("MISC.From the business").getItem(0).alias("From_the_business")),
    concat_ws(", ",col("MISC.Getting here").getItem(0).alias("Getting_here")),
    concat_ws(", ",col("MISC.Health & safety").getItem(0).alias("Health_and_safety")),
    concat_ws(", ",col("MISC.Highlights").getItem(0).alias("Highlights")),
    concat_ws(", ",col("MISC.Offerings").getItem(0).alias("Offerings")),
    concat_ws(", ",col("MISC.Payments").getItem(0).alias("Payments")),
    concat_ws(", ",col("MISC.Planning").getItem(0).alias("Planning")),
    concat_ws(", ",col("MISC.Popular for").getItem(0).alias("Popular_for")),
    concat_ws(", ",col("MISC.Recycling").getItem(0).alias("Recycling")),
    concat_ws(", ",col("MISC.Service options").getItem(0).alias("Service_options"))
)

'''
Segundo DataFrame
'''

# Se establece la ruta del archivo dentro del bucket
json_files_df_2 = ["gs://bucket_entrada_pf/metadata-sitios/4.json", "gs://bucket_entrada_pf/metadata-sitios/5.json",
                   "gs://bucket_entrada_pf/metadata-sitios/7.json", "gs://bucket_entrada_pf/metadata-sitios/8.json",
                   "gs://bucket_entrada_pf/metadata-sitios/10.json", "gs://bucket_entrada_pf/metadata-sitios/11.json"]

# Se crea una lista para lamcenar los DataFrames
dataframes_df_2 = []

# Se lee los archivos para convertirlos a DataFrame
for archivo in (json_files_df_2):
      df = spark.read.json(archivo)

      # Se agrega el DataFrame a la lista
      dataframes_df_2.append(df)
      
# Se combinan todos los dataframes en uno
df_combinado_2 = dataframes_df_2[0]  # Se tomar el primer DataFrame como base
for i in range(1, len(dataframes_df_2)):
    df_combinado_2 = df_combinado_2.union(dataframes_df_2[i])
    
'''
Transformación individual del DataFrame
'''

# Se añade una nueva columna que convierta la columna "hours" a un formato de string
df_expandido_2 = df_combinado_2.withColumn(
    "hours_str",
    concat_ws(", ", expr("transform(hours, x -> concat_ws(', ', x))")))

# Se expanden las columnas del DataFrame al tiempo que se filtra campos relevantes
df_expandido_2 = df_expandido_2.select(
    col("address"),
    col("avg_rating"),
    col("description"),
    col("gmap_id"),
    col("latitude"),
    col("longitude"),
    col("name"),
    col("num_of_reviews"),
    col("state"),
    col("hours_str").alias("Hours"),
    # Columnas sin la estructura MISC
    concat_ws(", ", col("category").alias("Category")),
    concat_ws(", ", col("relative_results").alias("Relative_Results")),
    # Columnas de la estructura MISC
    concat_ws(", ", col("MISC.Accessibility").getItem(0).alias("Accessibility")),
    concat_ws(", ", col("MISC.Activities").getItem(0).alias("Activities")),
    concat_ws(", ", col("MISC.Amenities").getItem(0).alias("Amenities")),
    concat_ws(", ", col("MISC.Atmosphere").getItem(0).alias("Atmosphere")),
    concat_ws(", ", col("MISC.Crowd").getItem(0).alias("Crowd")),
    concat_ws(", ", col("MISC.Dining options").getItem(0).alias("Dining_options")),
    concat_ws(", ", col("MISC.From the business").getItem(0).alias("From_the_business")),
    concat_ws(", ",col("MISC.Getting here").getItem(0).alias("Getting_here")),
    concat_ws(", ",col("MISC.Health & safety").getItem(0).alias("Health_and_safety")),
    concat_ws(", ",col("MISC.Highlights").getItem(0).alias("Highlights")),
    concat_ws(", ",col("MISC.Offerings").getItem(0).alias("Offerings")),
    concat_ws(", ",col("MISC.Payments").getItem(0).alias("Payments")),
    concat_ws(", ",col("MISC.Planning").getItem(0).alias("Planning")),
    concat_ws(", ",col("MISC.Popular for").getItem(0).alias("Popular_for")),
    concat_ws(", ",col("MISC.Recycling").getItem(0).alias("Recycling")),
    concat_ws(", ",col("MISC.Service options").getItem(0).alias("Service_options"))
)

'''
DataFrame Unido
'''

# Se unen los dos DataFrames expandidos
df_unido = df_expandido_1.union(df_expandido_2)

# Se filtra el DataFrame Unido por la categoria que contenga “restaurant”
df_filtrado = df_unido.filter(col("concat_ws(, , category AS Category)").like("%restaurant%"))

'''
Transformación del DataFrame
'''

# Se verifica que no existan filas duplicadas
df_filtrado = df_filtrado.dropDuplicates() # Se redefine el DataFrame aplicandole un `dropDuplicates()` para eliminar las filas duplicadas

'''
Normalizar los nombres
'''

# Se renombran las columnas
df_filtrado = df_filtrado.withColumnRenamed("address", "Address")
df_filtrado = df_filtrado.withColumnRenamed("avg_rating", "Avg_Rating")
df_filtrado = df_filtrado.withColumnRenamed("description", "Description")
df_filtrado = df_filtrado.withColumnRenamed("gmap_id", "Gmap_Id")
df_filtrado = df_filtrado.withColumnRenamed("latitude", "Latitude")
df_filtrado = df_filtrado.withColumnRenamed("longitude", "Longitude")
df_filtrado = df_filtrado.withColumnRenamed("name", "Name")
df_filtrado = df_filtrado.withColumnRenamed("num_of_reviews", "Number_of_Reviews")
df_filtrado = df_filtrado.withColumnRenamed("state", "State")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , category AS Category)", "Category")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , relative_results AS Relative_Results)", "Relative_Results")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Accessibility[0] AS Accessibility)", "Accessibility")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Activities[0] AS Activities)", "Activities")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Amenities[0] AS Amenities)", "Amenities")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Atmosphere[0] AS Atmosphere)", "Atmosphere")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Crowd[0] AS Crowd)", "Crowd")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Dining options[0] AS Dining_options)", "Dining_options")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.From the business[0] AS From_the_business)", "From_the_business")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Getting here[0] AS Getting_here)", "Getting_here")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Health & safety[0] AS Health_and_safety)", "Health_and_safety")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Highlights[0] AS Highlights)", "Highlights")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Offerings[0] AS Offerings)", "Offerings")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Payments[0] AS Payments)", "Payments")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Planning[0] AS Planning)", "Planning")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Popular for[0] AS Popular_for)", "Popular_for")
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Recycling[0] AS Recycling)", 'Recycling')
df_filtrado = df_filtrado.withColumnRenamed("concat_ws(, , MISC.Service options[0] AS Service_options)", "Service_options")

'''
DataFrame attributes
'''

# Se seleccionan las columnas a usar para el DataFrame
df_attributes = df_filtrado.select(
    "Gmap_Id",
    "Accessibility",
    "Activities",
    "Amenities",
    "Atmosphere",
    "Crowd",
    "Dining_options",
    "From_the_business",
    "Getting_here",
    "Health_and_safety",
    "Highlights",
    "Offerings",
    "Payments",
    "Planning",
    "Popular_for",
    "Recycling",
    "Service_options",
    "Avg_Rating"
)

'''
DataFrame metadata
'''

# Se seleccionan las columnas a usar para el DataFrame
df_metadata = df_filtrado.select(
    "Gmap_Id",
    "Name",
    "Avg_Rating",
    "Description",
    "Number_of_Reviews",
    "Address",
    "Latitude",
    "Longitude",
    "Category",
    "State",
    "Hours",
    "Relative_Results"
    )

'''
Exportar los DataFrame 
'''

# Se guarda el esquema del DataFrame metadata en una variable
schema_m = df_metadata.schema

# Se define una ruta para guardar el archivo
ruta_guardado_m = "gs://prometheus_bigquery/google_metadata"

# Se guarda el DataFrame en formato parquet sin particiones y se especifica la sobre escritura
df_metadata.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema_m).save(ruta_guardado_m)

# Se guarda el esquema del DataFrame attributes en una variable
schema_a = df_attributes.schema

# Se define una ruta para guardar el archivo
ruta_guardado_a = "gs://prometheus_bigquery/google_attributes"

# Se guarda el DataFrame en formato parquet sin particiones y se especifica la sobre escritura
df_attributes.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema_a).save(ruta_guardado_a)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus_sc/ETL_functions/etl_google_metadata.py  