import functions_framework
from google.cloud import bigquery
from google.cloud import storage

#Se establece el nombre del proyecto y del dataset en BigQuery
project_name = 'prometheus-data-solutions'
bigquery_dataset = 'Datawarehouse_prometheus'

#Esquemas para las tablas en BigQuery
schemas_id = {'google_reviews': [
                bigquery.SchemaField("Gmap_Id", "STRING"),
                bigquery.SchemaField("User_Id", "STRING"),
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("Date", "TIMESTAMP"),
                bigquery.SchemaField("Comment", "STRING"),
                bigquery.SchemaField("Rating", "INTEGER"),
                bigquery.SchemaField("Resp_Date", "TIMESTAMP"),
                bigquery.SchemaField("Resp_Comment", "STRING"),],
                'google_metadata': [
                bigquery.SchemaField("Gmap_Id", "STRING"),
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("Avg_Rating", "FLOAT64"),
                bigquery.SchemaField("Description", "STRING"),
                bigquery.SchemaField("Number_of_Reviews", "INTEGER"),
                bigquery.SchemaField("Address", "STRING"),
                bigquery.SchemaField("Latitude", "FLOAT64"),
                bigquery.SchemaField("Longitude", "FLOAT64"),
                bigquery.SchemaField("Category", "STRING"),
                bigquery.SchemaField("State", "STRING"),
                bigquery.SchemaField("Hours", "STRING"),
                bigquery.SchemaField("Relative_Results", "STRING")],
                'google_attributes': [
                bigquery.SchemaField("Gmap_Id", "STRING"),
                bigquery.SchemaField("Accessibility", "STRING"),
                bigquery.SchemaField("Activities", "STRING"),
                bigquery.SchemaField("Amenities", "STRING"),
                bigquery.SchemaField("Atmosphere", "STRING"),
                bigquery.SchemaField("Crowd", "STRING"),
                bigquery.SchemaField("Dining_options", "STRING"),
                bigquery.SchemaField("From_the_business", "STRING"),
                bigquery.SchemaField("Getting_here", "STRING"),
                bigquery.SchemaField("Health_and_safety", "STRING"),
                bigquery.SchemaField("Highlights", "STRING"),
                bigquery.SchemaField("Offerings", "STRING"),
                bigquery.SchemaField("Payments", "STRING"),
                bigquery.SchemaField("Planning", "STRING"),
                bigquery.SchemaField("Popular_for", "STRING"),
                bigquery.SchemaField("Recycling", "STRING"),
                bigquery.SchemaField("Service_options", "STRING"),
                bigquery.SchemaField("Avg_Rating", "FLOAT64")],
                'yelp_user': [
                bigquery.SchemaField("User_Id", "STRING"),
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("Yelping_Since", "TIMESTAMP"),
                bigquery.SchemaField("Elite", "STRING"),
                bigquery.SchemaField("Review_Count", "INTEGER"),
                bigquery.SchemaField("Average_Stars", "STRING"),
                bigquery.SchemaField("Useful", "INTEGER"),
                bigquery.SchemaField("Funny", "INTEGER"),
                bigquery.SchemaField("Cool", "INTEGER"),
                bigquery.SchemaField("Fans", "INTEGER"),
                bigquery.SchemaField("Compliment_Hot", "INTEGER"),
                bigquery.SchemaField("Compliment_More", "INTEGER"),
                bigquery.SchemaField("Compliment_Profile", "INTEGER"),
                bigquery.SchemaField("Compliment_Cute", "INTEGER"),
                bigquery.SchemaField("Compliment_List", "INTEGER"),
                bigquery.SchemaField("Compliment_Note", "INTEGER"),
                bigquery.SchemaField("Compliment_Plain", "INTEGER"),
                bigquery.SchemaField("Compliment_Cool", "INTEGER"),
                bigquery.SchemaField("Compliment_Funny", "INTEGER"),
                bigquery.SchemaField("Compliment_Writer", "INTEGER"),
                bigquery.SchemaField("Compliment_Photos", "INTEGER"),
                bigquery.SchemaField("Friends", "STRING")],
                'yelp_reviews': [
                bigquery.SchemaField("Business_Id", "STRING"),
                bigquery.SchemaField("User_Id", "STRING"),
                bigquery.SchemaField("Date", "TIMESTAMP"),
                bigquery.SchemaField("Comment", "STRING"),
                bigquery.SchemaField("Cool", "INTEGER"),
                bigquery.SchemaField("Funny", "INTEGER"),
                bigquery.SchemaField("Useful", "INTEGER"),
                bigquery.SchemaField("Stars", "FLOAT64")],
                'yelp_tip': [
                bigquery.SchemaField("Business_Id", "STRING"),
                bigquery.SchemaField("User_Id", "STRING"),
                bigquery.SchemaField("Date", "TIMESTAMP"),
                bigquery.SchemaField("Comment", "STRING"),
                bigquery.SchemaField("Compliment_Count", "INTEGER")],
                'yelp_checkin': [
                bigquery.SchemaField("Business_Id", "STRING"),
                bigquery.SchemaField("Date", "TIMESTAMP")],
                'yelp_attributes': [
                bigquery.SchemaField("Business_Id", "STRING"),
                bigquery.SchemaField("Stars", "FLOAT64"),
                bigquery.SchemaField("RestaurantsDelivery", "STRING"),
                bigquery.SchemaField("RestaurantsTakeOut", "STRING"),
                bigquery.SchemaField("RestaurantsPriceRange2", "STRING"),
                bigquery.SchemaField("RestaurantsGoodForGroups", "STRING"),
                bigquery.SchemaField("OutdoorSeating", "STRING"),
                bigquery.SchemaField("RestaurantsReservations", "STRING"),
                bigquery.SchemaField("GoodForKids", "STRING"),
                bigquery.SchemaField("BusinessAcceptsCreditCards", "STRING"),
                bigquery.SchemaField("HasTV", "STRING"),
                bigquery.SchemaField("Alcohol", "STRING"),
                bigquery.SchemaField("Garage", "STRING"),
                bigquery.SchemaField("Street", "STRING"),
                bigquery.SchemaField("Validated", "STRING"),
                bigquery.SchemaField("Lot", "STRING"),
                bigquery.SchemaField("Valet", "STRING")],
                'yelp_business': [
                bigquery.SchemaField("Business_Id", "STRING"),
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("Address", "STRING"),
                bigquery.SchemaField("City", "STRING"),
                bigquery.SchemaField("State", "STRING"),
                bigquery.SchemaField("Postal_Code", "STRING"),
                bigquery.SchemaField("Latitude", "FLOAT64"),
                bigquery.SchemaField("Longitude", "FLOAT64"),
                bigquery.SchemaField("Stars", "FLOAT64"),
                bigquery.SchemaField("Review_count", "INTEGER"),
                bigquery.SchemaField("Is_open", "INTEGER"),
                bigquery.SchemaField("Categories", "STRING")]}


#Función principal de la Cloud Function
def cargar_dataset(event,context):

     #Se obtiene información sobre el archivo que desencadenó la función
    file = event
    file_name=file['name']   
    table_name = file_name.split("/")[0] 

    #Se verifica si el archivo es un archivo Parquet
    if file_name.endswith(".parquet"):
        
        print(f"Se detectó que se subió el archivo {file_name} en el bucket {file['bucket']}.")

         #Se obtiene el nombre del bucket fuente y se crea un cliente de BigQuery
        source_bucket_name = file['bucket'] 
        client = bigquery.Client()

        #Se construye la ruta y el ID de la tabla en BigQuery
        source_path = "gs://"+source_bucket_name+"/"+file_name
        table_id = project_name + "." + bigquery_dataset + "." + table_name 
        
        #Configuración del trabajo de carga en BigQuery
        job_config = bigquery.LoadJobConfig(
            schema= schemas_id[table_name],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        
            source_format=bigquery.SourceFormat.PARQUET,
        )

        uri = source_path

        #Se cargan los datos en BigQuery
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        destination_table = client.get_table(table_id) 
        print("Loaded {} rows.".format(destination_table.num_rows))

        #Se realizan acciones adicionales según el tipo de tabla
        if table_name == 'google_reviews':
            
            #Si la tabla es google_reviews, se realiza una consulta SQL deseada
            query= """
                INSERT INTO `Datawarehouse_prometheus.google_nlp`
                SELECT
                gr.Gmap_Id,
                gm.name,
                gr.date,
                gr.comment,
                gr.rating AS stars,
                'google' AS source
                FROM
                Datawarehouse_prometheus.google_reviews gr
                JOIN
                Datawarehouse_prometheus.google_metadata gm
                ON
                gr.gmap_id = gm.gmap_id;

                TRUNCATE TABLE prometheus-data-solutions.Datawarehouse_prometheus.google_reviews;

            """            
            query_job = client.query(query)
            query_job.result()
            print("se ejecuto la consulta SQL")

        if table_name == 'yelp_reviews':
            
            #Si la tabla es yelp_reviews, se realiza una consulta SQL deseada
            query= """
                INSERT INTO `Datawarehouse_prometheus.yelp_nlp`
                SELECT
                yr.business_id,
                yb.name,
                yr.date,
                yr.comment,
                CAST(yr.stars AS INT64) AS stars,
                'yelp' AS source
                FROM
                Datawarehouse_prometheus.yelp_reviews yr
                JOIN
                Datawarehouse_prometheus.yelp_business yb
                ON
                yr.Business_Id = yb.Business_Id;

                TRUNCATE TABLE prometheus-data-solutions.Datawarehouse_prometheus.yelp_reviews;

            """            
            query_job = client.query(query)
            query_job.result()
            print("se ejecuto la consulta SQL")

        #Se elimina el archivo del bucket después de la carga en BigQuery
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(source_bucket_name)
            blob = bucket.blob(file_name)
            blob.delete()
            print(f"Archivo {file_name} eliminado del bucket {source_bucket_name}.")
            blob = bucket.blob(table_name+'/__SUCCESS')
            blob.delete()
            print(f"Archivo __SUCCESS eliminado del bucket {source_bucket_name}.")
        except Exception as e:
            print(f"Error al eliminar el archivo del bucket: {str(e)}")
    else:
        print(f"Ignorando archivo {file_name} que no es un archivo Parquet.")
    
