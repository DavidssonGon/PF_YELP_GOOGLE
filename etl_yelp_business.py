'''
ETL YELP BUSINESS
En este script de python se realizará un proceso de ETL por medio de diferentes funciones
utilizando el framework de Spark para python pysaprk.
'''

# Se importan funciones específicas 
from pyspark.sql import SparkSession # Se importa SparkSession
from pyspark import SparkContext # Se importa SparkContext
from pyspark.sql import functions as F # Se importan las funciones de spark
from pyspark.sql.types import * # Se importan los tipos de datos
from pyspark.sql import DataFrame # Se importa explicitamente la clase DataFrame
from pyspark.sql.functions import col, explode, concat_ws, expr, regexp_replace, from_unixtime # Se importa explicitamente la funcion col y regexp_replace
spark = SparkSession.builder.master("local[*]").getOrCreate() # Se define la variable spark construyendo una sesion en la maquina de colab

# Se importan las librerías necesarias
import pickle
import numpy as np
import pandas as pd
import ast
import gcsfs

# Se establece la ruta del archivo dentro del bucket
ruta = 'gs://bucket_entrada_pf/business.pkl'

# Se abre el archivo pickle en modo binario
with gcsfs.GCSFileSystem().open(ruta, "rb") as archivo:
  business_1 = pickle.load(archivo) # Se cargan los datos del archivo pickle
  
'''
Transformación del DataFrame incial
'''

# Se filtra el DataFrame quitando columnas repetidas
business_1 = business_1.iloc[:, :13]

# Se establece el tipo de dato del campo “categories” como tipo string
business_1['categories'] = business_1['categories'].astype(str)

# Se filtra el DataFrame nuevamente para extraer la categoría “Restaurants”
business_2 = business_1[business_1['categories'].str.contains('Restaurants')]

# Se filtra esta vez por el estado de Pensilvania
business_3 = business_2[business_2['state'] == 'PA']

# Se filtra el DataFrame final de business sin el campo “attributes”
business = business_3.loc[:, business_3.columns != 'attributes']

# Se reinicia el conteo de las columnas por medio del índice
business.reset_index(inplace=True)

# Se elimina la columna index
business = business.drop(columns=['index'])

'''
Creación del DataFrame attributes
'''

# Se crea el DataFrame “attributes”
attributes=business_3[['business_id','attributes']]
attributes.reset_index(inplace=True)
attributes = attributes.drop(columns=['index'])

# Se eliminan las filas con valores nulos
attributes_completo = attributes.dropna()
df_nan = attributes[pd.isna(attributes).any(axis=1)]

'''
De diccionarios a columnas
'''

# Se convierten los diccionarios dentro del campo “attributes” en listas
df_normalizado = attributes['attributes'].apply(pd.Series)

# Se concatena el campo “business_id” del DataFrame original con el normalizado
df_resultado = pd.concat([attributes['business_id'], df_normalizado], axis=1)

'''
Detección de columnas con diccionarios
'''

# Se detecta la columna que contenga el símbolo “{“
columna_con_simbolo = df_resultado.apply(lambda col: col.astype(str).str.contains('{').any())

# Se obtienen los nombres de las columnas que contengan el simbolo “{“
columna_con_simbolo = columna_con_simbolo[columna_con_simbolo].index.to_list()

'''
Columna Music
'''

# Se crea un DataFrame de este campo
df_music = df_resultado[['business_id', 'Music']]

# Se sustituyen los valores NaN por un marcador de posición (por ejemplo, 'N/A')
df_music['Music'] = df_music['Music'].fillna('N/A')

# Se filtra el DataFrame con valores que contengan el símbolo “{“
df_music2 = df_music[df_music['Music'].str.contains('{')]

# Se filtra el DataFrame con valores que no contengan el símbolo “{“
df_music3 = df_music[~df_music['Music'].str.contains('{')]

# Se convierte el campo “Music” en diccionarios utilizando ast.literal_eval
df_music2['Music'] = df_music2['Music'].apply(ast.literal_eval)

# Se utiliza apply junto con pd.Series para convertir diccionarios en listas
df_normalizado = pd.concat([df_music2['business_id'], df_music2['Music'].apply(pd.Series)], axis=1)

# Se crea una copia del df_normalizado para preservar el DataFrame original
df_apendiado = df_normalizado.copy()

# Se establece la columna “Music” de df_apendiado en NaN
df_apendiado['Music'] = np.nan

# Se concatenan las filas de df_music3, seleccionando únicamente las columnas "business_id" y "Music"
df_apendiado = pd.concat([df_apendiado, df_music3[['business_id', 'Music']]], ignore_index=True)

# Se elimina la columna original de “Music”
df_apendiado.drop(columns=['Music'], inplace=True)
df_resultado.drop(columns=['Music'], inplace=True)

# Se realiza un Inner Join de los DataFrame resultantes
resultado_df_music = pd.merge(df_resultado, df_apendiado, on='business_id', how='inner')

'''
Columna Ambience
'''

# Se crea un DataFrame de este campo
df_ambience = df_resultado[['business_id', 'Ambience']]

# Se sustituyen los valores NaN por un marcador de posición (por ejemplo, 'N/A')
df_ambience['Ambience'] = df_ambience['Ambience'].fillna('N/A')

# Se filtra el DataFrame con valores que contengan el símbolo “{“
df_ambience2 = df_ambience[df_ambience['Ambience'].str.contains('{')]

# Se filtra el DataFrame con valores que no contengan el símbolo “{“
df_ambience3 = df_ambience[~df_ambience['Ambience'].str.contains('{')]

# Se convierte el campo “Ambience” en diccionarios utilizando ast.literal_eval
df_ambience2['Ambience'] = df_ambience2['Ambience'].apply(ast.literal_eval)

# Se utiliza apply junto con pd.Series para convertir diccionarios en listas
df_normalizado = pd.concat([df_ambience2['business_id'], df_ambience2['Ambience'].apply(pd.Series)], axis=1)

# Se crea una copia del df_normalizado para preservar el DataFrame original
df_apendiado = df_normalizado.copy()

# Se establece la columna “Ambience” de df_apendiado en NaN
df_apendiado['Ambience'] = np.nan

# Se concatenan las filas de df_ambience3, seleccionando únicamente las columnas "business_id" y "Ambience"
df_apendiado = pd.concat([df_apendiado, df_ambience3[['business_id', 'Ambience']]], ignore_index=True)

# Se elimina la columna "Ambience" del DataFrame anterior
resultado_df_music.drop(columns=['Ambience'], inplace=True)

# Se realiza un Inner Join de los DataFrame resultantes
resultado_df_ambience = pd.merge(resultado_df_music, df_apendiado, on='business_id', how='inner')

'''
Columna goodformeal
'''

# Se crea un DataFrame de este campo
df_goodformeal = df_resultado[['business_id', 'GoodForMeal']]

# Se sustituyen los valores NaN por un marcador de posición (por ejemplo, 'N/A')
df_goodformeal['GoodForMeal'] = df_goodformeal['GoodForMeal'].fillna('N/A')

# Se filtra el DataFrame con valores que contengan el símbolo “{“
df_goodformeal2 = df_goodformeal[df_goodformeal['GoodForMeal'].str.contains('{')]

# Se filtra el DataFrame con valores que no contengan el símbolo “{“
df_goodformeal3 = df_goodformeal[~df_goodformeal['GoodForMeal'].str.contains('{')]

# Se convierte el campo “GoodForMeal” en diccionarios utilizando ast.literal_eval
df_goodformeal2['GoodForMeal'] = df_goodformeal2['GoodForMeal'].apply(ast.literal_eval)

# Se utiliza apply junto con pd.Series para convertir diccionarios en listas
df_normalizado = pd.concat([df_goodformeal2['business_id'], df_goodformeal2['GoodForMeal'].apply(pd.Series)], axis=1)

# Se crea una copia del df_normalizado para preservar el DataFrame original
df_apendiado = df_normalizado.copy()

# Se establece la columna “GoodForMeal” de df_apendiado en NaN
df_apendiado['GoodForMeal'] = np.nan

# Se concatenan las filas de df_ambience3, seleccionando únicamente las columnas "business_id" y "GoodForMeal"
df_apendiado = pd.concat([df_apendiado, df_goodformeal3[['business_id', 'GoodForMeal']]], ignore_index=True)

# Se elimina la columna "GoodForMeal" del DataFrame df_apendiado
df_apendiado.drop(columns=['GoodForMeal'], inplace=True)

# Se elimina la columna "GoodForMeal" del DataFrame anterior
resultado_df_ambience.drop(columns=['GoodForMeal'], inplace=True)

# Se realiza un Inner Join de los DataFrame resultantes
resultado_df_goodformeal = pd.merge(resultado_df_ambience, df_apendiado, on='business_id', how='inner')

'''
Columna businessparking
'''

# Se crea un DataFrame de este campo
df_businessparking = df_resultado[['business_id', 'BusinessParking']]

# Se sustituyen los valores NaN por un marcador de posición (por ejemplo, 'N/A')
df_businessparking['BusinessParking'] = df_businessparking['BusinessParking'].fillna('N/A')

# Se filtra el DataFrame con valores que contengan el símbolo “{“
df_businessparking2 = df_businessparking[df_businessparking['BusinessParking'].str.contains('{')]

# Se filtra el DataFrame con valores que no contengan el símbolo “{“
df_businessparking3 = df_businessparking[~df_businessparking['BusinessParking'].str.contains('{')]

# Se convierte el campo “BusinessParking” en diccionarios utilizando ast.literal_eval
df_businessparking2['BusinessParking'] = df_businessparking2['BusinessParking'].apply(ast.literal_eval)

# Se utiliza apply junto con pd.Series para convertir diccionarios en listas
df_normalizado = pd.concat([df_businessparking2['business_id'], df_businessparking2['BusinessParking'].apply(pd.Series)], axis=1)

# Se crea una copia del df_normalizado para preservar el DataFrame original
df_apendiado = df_normalizado.copy()

# Se establece la columna “BusinessParking” de df_apendiado en NaN
df_apendiado['BusinessParking'] = np.nan

# Se concatenan las filas de df_ambience3, seleccionando únicamente las columnas "business_id" y "BusinessParking"
df_apendiado = pd.concat([df_apendiado, df_businessparking3[['business_id', 'BusinessParking']]], ignore_index=True)

# Se elimina la columna "BusinessParking" del DataFrame df_apendiado
df_apendiado.drop(columns=['BusinessParking'], inplace=True)

# Se elimina la columna "BusinessParking" del DataFrame anterior
resultado_df_goodformeal.drop(columns=['BusinessParking'], inplace=True)

# Se realiza un Inner Join de los DataFrame resultantes
resultado_df_businessparking = pd.merge(resultado_df_goodformeal, df_apendiado, on='business_id', how='inner')

'''
Columna bestnights
'''

# Se crea un DataFrame de este campo
df_bestnights = df_resultado[['business_id', 'BestNights']]

# Se sustituyen los valores NaN por un marcador de posición (por ejemplo, 'N/A')
df_bestnights['BestNights'] = df_bestnights['BestNights'].fillna('N/A')

# Se filtra el DataFrame con valores que contengan el símbolo “{“
df_bestnights2 = df_bestnights[df_bestnights['BestNights'].str.contains('{')]

# Se filtra el DataFrame con valores que no contengan el símbolo “{“
df_bestnights3 = df_bestnights[~df_bestnights['BestNights'].str.contains('{')]

# Se convierte el campo “BestNights” en diccionarios utilizando ast.literal_eval
df_bestnights2['BestNights'] = df_bestnights2['BestNights'].apply(ast.literal_eval)

# Se utiliza apply junto con pd.Series para convertir diccionarios en listas
df_normalizado = pd.concat([df_bestnights2['business_id'], df_bestnights2['BestNights'].apply(pd.Series)], axis=1)

# Se crea una copia del df_normalizado para preservar el DataFrame original
df_apendiado = df_normalizado.copy()

# Se establece la columna “BestNights” de df_apendiado en NaN
df_apendiado['BestNights'] = np.nan

# Se concatenan las filas de df_ambience3, seleccionando únicamente las columnas "business_id" y "BestNights"
df_apendiado = pd.concat([df_apendiado, df_bestnights3[['business_id', 'BestNights']]], ignore_index=True)

# Se elimina la columna "BestNights" del DataFrame df_apendiado
df_apendiado.drop(columns=['BestNights'], inplace=True)

# Se elimina la columna "BestNights" del DataFrame anterior
resultado_df_businessparking.drop(columns=['BestNights'], inplace=True)

# Se realiza un Inner Join de los DataFrame resultantes
resultado_df_bestnights = pd.merge(resultado_df_businessparking, df_apendiado, on='business_id', how='inner')

'''
Columna dietaryrestrictions
'''

# Se crea un DataFrame de este campo
df_dietaryrestrictions = df_resultado[['business_id', 'DietaryRestrictions']]

# Se sustituyen los valores NaN por un marcador de posición (por ejemplo, 'N/A')
df_dietaryrestrictions['DietaryRestrictions'] = df_dietaryrestrictions['DietaryRestrictions'].fillna('N/A')

# Se filtra el DataFrame con valores que contengan el símbolo “{“
df_dietaryrestrictions2 = df_dietaryrestrictions[df_dietaryrestrictions['DietaryRestrictions'].str.contains('{')]

# Se filtra el DataFrame con valores que no contengan el símbolo “{“
df_dietaryrestrictions3 = df_dietaryrestrictions[~df_dietaryrestrictions['DietaryRestrictions'].str.contains('{')]

# Se convierte el campo “DietaryRestrictions” en diccionarios utilizando ast.literal_eval
df_dietaryrestrictions2['DietaryRestrictions'] = df_dietaryrestrictions2['DietaryRestrictions'].apply(ast.literal_eval)

# Se utiliza apply junto con pd.Series para convertir diccionarios en listas
df_normalizado = pd.concat([df_dietaryrestrictions2['business_id'], df_dietaryrestrictions2['DietaryRestrictions'].apply(pd.Series)], axis=1) 

# Se crea una copia del df_normalizado para preservar el DataFrame original
df_apendiado = df_normalizado.copy()

# Se establece la columna “DietaryRestrictions” de df_apendiado en NaN
df_apendiado['DietaryRestrictions'] = np.nan

# Se concatenan las filas de df_ambience3, seleccionando únicamente las columnas "business_id" y "DietaryRestrictions"
df_apendiado = pd.concat([df_apendiado, df_dietaryrestrictions3[['business_id', 'DietaryRestrictions']]], ignore_index=True)

# Se elimina la columna "DietaryRestrictions" del DataFrame df_apendiado
df_apendiado.drop(columns=['DietaryRestrictions'], inplace=True)

# Se elimina la columna "DietaryRestrictions" del DataFrame anterior
resultado_df_bestnights.drop(columns=['DietaryRestrictions'], inplace=True)

# Se realiza un Inner Join de los DataFrame resultantes
resultado_df_dietaryrestrictions = pd.merge(resultado_df_bestnights, df_apendiado, on='business_id', how='inner')

'''
DataFrame attributes
'''

# Se define el DataFrame attributes
df_attributes = pd.concat([resultado_df_dietaryrestrictions, df_nan], axis=0)

# Se elimina la columna “'HairSpecializesIn”
df_attributes.drop(columns=['HairSpecializesIn'], inplace=True)

# Se trae el campo estrellas para incluirlo al DataFrame
stars = business[['business_id', 'stars']]

# Se realiza un merge para incluir el campo “stars” en el DataFrame
df_attributes = pd.merge(stars, df_attributes, on='business_id', how='inner')

# Se realiza una limpieza de columnas que cuentan con más de un 75% de sus valores nulos
columnas_faltantes = df_attributes.isna().sum()

# Lista para almacenar los nombres de las columnas con más de 5838 valores faltantes
columnas_eliminar = []

# Contador para contar las columnas que cumplen la condición
c = 0

# Iterar sobre los pares clave-valor (nombre de la columna y cantidad de valores faltantes)
for column, count in columnas_faltantes.items():
    if count > 2919:
        c = c+1
        columnas_eliminar.append(column)

# Se eliminan las columnas establecidas
df_attributes = df_attributes.drop(columns=columnas_eliminar)

'''
Convertir de Pandas a Spark
''' 

# Se define el nuevo DataFrame en Spark
df_yelp_business = spark.createDataFrame(business)

# Se modifican los tipos de datos booleanos al DataFrame attributes de Pandas
string_columns = ["RestaurantsDelivery", 'RestaurantsTakeOut', "RestaurantsGoodForGroups", 
                   "OutdoorSeating", "RestaurantsReservations", "GoodForKids", 
                   "BusinessAcceptsCreditCards", "HasTV", "garage", "street", 
                   "validated", "lot", "valet"]

# Se convierte las columnas booleanas de tipo str a bool
df_attributes[string_columns] = df_attributes[string_columns].astype(bool)

# Se modifica el tipo de dato del campo “RestaurantsPriceRange2” de pandas
df_attributes["RestaurantsPriceRange2"] = pd.to_numeric(df_attributes["RestaurantsPriceRange2"], errors='coerce')

# Se define el esquema que tendrá el DataFrame de Spark
schema_attributes = StructType([
    StructField("business_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("RestaurantsDelivery", StringType(), True),
    StructField("RestaurantsTakeOut", StringType(), True),
    StructField("RestaurantsPriceRange2", StringType(), True),
    StructField("RestaurantsGoodForGroups", StringType(), True),
    StructField("OutdoorSeating", StringType(), True),
    StructField("RestaurantsReservations", StringType(), True),
    StructField("GoodForKids", StringType(), True),
    StructField("BusinessAcceptsCreditCards", StringType(), True),
    StructField("HasTV", StringType(), True),
    StructField("Alcohol", StringType(), True),
    StructField("garage", StringType(), True),
    StructField("street", StringType(), True),
    StructField("validated", StringType(), True),
    StructField("lot", StringType(), True),
    StructField("valet", StringType(), True)
    ])

# Se define el nuevo DataFrame en Spark
df_yelp_attributes = spark.createDataFrame(df_attributes, schema=schema_attributes)

'''
Transformación de DataFrames
'''

# Se renombran las columnas
df_yelp_business = df_yelp_business.withColumnRenamed("business_id", "Business_Id")
df_yelp_business = df_yelp_business.withColumnRenamed("name", "Name")
df_yelp_business = df_yelp_business.withColumnRenamed("address", "Address")
df_yelp_business = df_yelp_business.withColumnRenamed("city", "City")
df_yelp_business = df_yelp_business.withColumnRenamed("state", "State")
df_yelp_business = df_yelp_business.withColumnRenamed("postal_code", "Postal_Code")
df_yelp_business = df_yelp_business.withColumnRenamed("latitude", "Latitude")
df_yelp_business = df_yelp_business.withColumnRenamed("longitude", "Longitude")
df_yelp_business = df_yelp_business.withColumnRenamed("stars", "Stars")
df_yelp_business = df_yelp_business.withColumnRenamed("review_count", "Review_count")
df_yelp_business = df_yelp_business.withColumnRenamed("is_open", "Is_open")
df_yelp_business = df_yelp_business.withColumnRenamed("categories", "Categories")

# Se renombran algunas columnas
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("business_id", "Business_Id")
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("stars", "Stars")
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("garage", "Garage")
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("street", "Street")
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("validated", "Validated")
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("lot", "Lot")
df_yelp_attributes = df_yelp_attributes.withColumnRenamed("valet", "Valet")

'''
Exportar los DataFrame 
'''

# Se guarda el esquema del DataFrame metadata en una variable
schema_a = df_yelp_attributes.schema

# Se define una ruta para guardar el archivo
ruta_guardado_a = "gs://prometheus_bigquery/yelp_attributes"

# Se guarda el DataFrame en formato parquet sin particiones y se especifica la sobre escritura
df_yelp_attributes.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema_a).save(ruta_guardado_a)

# Se guarda el esquema del DataFrame metadata en una variable
schema_b = df_yelp_business.schema

# Se define una ruta para guardar el archivo
ruta_guardado_b = "gs://prometheus_bigquery/yelp_business"

# Se guarda el DataFrame en formato parquet sin particiones y se especifica la sobre escritura

df_yelp_business.coalesce(1).write.mode("overwrite").format("parquet").options(schema=schema_b).save(ruta_guardado_b)

# Se imprime un mensaje de finalización de proceso
print("El proceso de ETL se ejecutó de manera exitosa")

# gs://bucket_prometheus_sc/ETL_functions/etl_yelp_business.py