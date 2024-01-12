import functions_framework
from google.cloud import bigquery

project_name = 'prometheus-data-solutions'
bigquery_dataset = 'Datawarehouse_prometheus'

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




def mover_dataset_x(event,context):
    #Se identifica el nombre de la carpeta que fue agregada al bucket
    file = event
    file_name=file['name']   
    table_name = file_name.split("/")[0] 
    
    #Se busca el archivo parquet dentro de la carpeta
    if file_name.endswith(".parquet"):
        print(f"Se detectó que se subió el archivo {file_name} en el bucket {file['bucket']}.")
        source_bucket_name = file['bucket'] 

    
        client = bigquery.Client()

        #Se crea la ruta al archivo cargado en el bucket
        source_path = "gs://"+source_bucket_name+"/"+file_name 
        table_id = project_name + "." + bigquery_dataset + "." + table_name 
        

        #Se carga el archivo en la tabla de bigquery        
        job_config = bigquery.LoadJobConfig(
            schema= schemas_id[table_name],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        
            source_format=bigquery.SourceFormat.PARQUET,
        )

        uri = source_path

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result() 

        destination_table = client.get_table(table_id) 
        print("Loaded {} rows.".format(destination_table.num_rows))
    #Se ignoran los archivos innecesarios
    else:
        print(f"Ignorando archivo {file_name} que no es un archivo Parquet.")
    
