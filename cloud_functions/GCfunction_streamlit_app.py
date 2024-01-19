import json
from flask import request
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from math import radians, sin, cos, sqrt, atan2
from google.cloud import bigquery
from datetime import datetime

# Configurar proyecto de Google Cloud
project_id = "prometheus-data-solutions"
client = bigquery.Client(project=project_id)

def solicitud_streamlit(request):
    if request.method == 'POST':
        nuevo_registro = request.get_json()
        print(nuevo_registro)

        try:
            df_attributes = client.query("SELECT * FROM prometheus-data-solutions.Datawarehouse_prometheus.yelp_ml;").to_dataframe()
        except Exception as e:
            print(f"Error: {e}")
            return "Error en la consulta"


        # Extraer los valores de 'Radius' y 'OrderBy' en variables aparte
        radius = nuevo_registro.pop('Radius', None)
        order_by = nuevo_registro.pop('OrderBy', None)

        #Se filtran los restaurantes que estan dentro del radio indicado por el usuario

        def haversine(lat1, lon1, lat2, lon2):
            # Convertir coordenadas de grados a radianes
            lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

            # Calcular las diferencias de latitud y longitud
            dlat = lat2 - lat1
            dlon = lon2 - lon1

            # Calcular la distancia haversine
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            radio_tierra_km = 6371  # Radio medio de la Tierra en kilómetros

            distancia = radio_tierra_km * c
            return distancia
        
        #Se filtra segun negocio validado y si acepta tarjeta de credito
        
        if nuevo_registro['BusinessAcceptsCreditCards']==True:
            # Valor de 'BusinessAcceptsCreditCards' en nuevo_registro
            valor = nuevo_registro['BusinessAcceptsCreditCards']

            # Filtrar registros en df_attributes basado en 'BusinessAcceptsCreditCards'
            df_attributes = df_attributes[df_attributes['BusinessAcceptsCreditCards'] == valor]

        if nuevo_registro['Validated']==True: 
            # Valor de 'Validated' en nuevo_registro
            valor = nuevo_registro['Validated']

            # Filtrar registros en df_attributes basado en 'Validated'
            df_attributes = df_attributes[df_attributes['Validated'] == valor]

        

        # Datos de nuevo_registro
        latitud_nuevo = nuevo_registro['Latitude']
        longitud_nuevo = nuevo_registro['Longitude']

        # Filtrar registros en df_attributes dentro del radio especificado y agregar columna de distancia
        df_attributes['Distance'] = df_attributes.apply(
            lambda row: round(haversine(latitud_nuevo, longitud_nuevo, row['Latitude'], row['Longitude']), 1), axis=1
        )

        # Filtrar registros dentro del radio especificado
        df_attributes = df_attributes[df_attributes['Distance'] <= radius]

        #Se inicia el proceso para calcular similitud de coseno

        # Crear un DataFrame con el nuevo registro
        df_nuevo_registro = pd.DataFrame([nuevo_registro])

        # Concatenar el nuevo DataFrame con df_attributes
        df_attributes = pd.concat([df_attributes, df_nuevo_registro], ignore_index=True)

        # Utilizar CountVectorizer para convertir las cadenas de texto en vectores numéricos
        vectorizer = CountVectorizer()

        #Se crea la matriz de términos-documentos
        genres_matrix = vectorizer.fit_transform(df_attributes['AllAttributes']).toarray()

        #Se calcula la similitud del coseno entre los juegos
        cosine_similarities = cosine_similarity(genres_matrix, genres_matrix)

        #Se obtiene el índice del registro ingresado por el usuario
        idx = df_attributes[df_attributes['Business_Id'] == 'solicitado'].index[0]

        #Se obtienen las puntuaciones de similitud del coseno para nuestra fila con respecto a todas las demás
        sim_scores = list(enumerate(cosine_similarities[idx]))

        #Se ordenan los restaurantes según sus puntuaciones de similitud
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

        #Se obtienen los índices de los restaurantes recomendados
        n=5 #cantidad de registros a obtener
        sim_scores = sim_scores[1:n+1]  # Excluyendo nuestro registro
        indices = [i[0] for i in sim_scores]

        #Devuelve la lista de restaurantes recomendados
        recomendados_id=df_attributes['Business_Id'].iloc[indices].tolist()

        # Filtrar registros en df_attributes que coincidan con los IDs en recomendados_id
        df_recomendados = df_attributes[df_attributes['Business_Id'].isin(recomendados_id)].copy()

        #Ordenamiento por cercania
        if order_by== 'distance':
            df_recomendados = df_recomendados.sort_values(by='Distance', ascending=True)

        #Ordenamiento por nlp
        if order_by== 'nlp':
            df_recomendados = df_recomendados.sort_values(by='SentimentAnalysis', ascending=False)

        df_recomendados['SentimentAnalysis']=round(df_recomendados['SentimentAnalysis'],1)

        # Seleccionar y renombrar las columnas deseadas
        df_recomendados = df_recomendados[['Name', 'Address', 'Distance','SentimentAnalysis','Latitude','Longitude']]
        df_recomendados = df_recomendados.rename(columns={
            'Name': 'Restaurante',
            'Address': 'Dirección',
            'Distance': 'Distancia(km)',
            'SentimentAnalysis': 'Análisis de sentimiento',
            'Latitude': 'Latitud',
            'Longitude': 'Longitud'
        })

        # Convertir el DataFrame a formato JSON
        json_recomendados = df_recomendados.to_json(orient='records')

        print(json.dumps({"df_recomendados": json_recomendados}))

        # Agregar fila a la tabla de BigQuery
        nueva_fila = {
            'mail': nuevo_registro['Name'],
            'recomendacion': json_recomendados,
            'date': datetime.utcnow()
        }

        table_ref = client.dataset('Datawarehouse_prometheus').table('registro_usuarios')
        table = client.get_table(table_ref)

        # Insertar la fila
        client.insert_rows(table, [nueva_fila])



        # Devolver el JSON como respuesta
        return json.dumps({"df_recomendados": json_recomendados})

    else:
        return {"error": "Método no permitido"}, 405
