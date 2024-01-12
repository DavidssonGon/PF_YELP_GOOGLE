# Yelp - Google Maps

## Objetivos del Proyecto
La Asociación de Restaurantes y Afines de Pennsylvania ha encomendado a Prometheus Data Solutions la tarea de evaluar la percepción de los clientes sobre los establecimientos afiliados, mediante el análisis de reseñas en Yelp y Google Maps. Nuestros objetivos principales incluyen medir la satisfacción de los clientes, identificar preferencias, señalar áreas de mejora, destacar sectores con oportunidades de crecimiento y anticipar aquellos que podrían enfrentar disminuciones en las ventas. Además, se busca diseñar un sistema de recomendación mediante Machine Learning que ofrezca sugerencias personalizadas a los usuarios, basándose en sus gustos y preferencias específicas.

## Alcance del Proyecto
El proyecto abarcará el proceso ETL y el Análisis Exploratorio de Datos (EDA), considerando la integración de información proveniente de Yelp y Google Maps. Se utilizarán herramientas como PySpark y Streamlit. El modelo de recomendación de Machine Learning incorporará análisis de sentimientos, técnicas de procesamiento de lenguaje natural (NLP) y considerará restaurantes de grandes cadenas en otros estados para mejorar la percepción en Pensilvania.

## Stack Tecnológico
### Lenguaje de Programación
- Python

### Bibliotecas
- Pandas, Matplotlib, Seaborn, NLTK

### Frameworks
- PySpark, Streamlit

### Plataforma de Cloud
- Google Cloud Platform (GCP)

### Servicios y Herramientas de GCP
- Cloud Storage (almacenamiento de objetos)
- Dataproc (procesamiento de datos en clústeres Apache Spark)
- BigQuery (Data Warehouse)
- Vertex AI (entrenamiento de modelos de ML)

### Endpoints
- Deployment Streamlit

## Dashboard

### Inspiración de Diseño

El diseño del dashboard se basará en la estética de Yelp para proporcionar al receptor un contexto visual coherente. Esto incluirá el uso de paletas de colores y tipografías inspiradas en Yelp.

Además, se prestará especial atención a aspectos de accesibilidad, llevando a cabo pruebas de daltonismo para elegir colores amigables y asegurando etiquetados adecuados para mejorar la experiencia de usuarios con diversas necesidades.

### Estructura del Dashboard

El dashboard constará de varias secciones:

1. **Home**
   - Página de inicio con un menú que facilitará la navegación por las diferentes secciones del dashboard.

2. **Contexto Geográfico**
   - Tablero que mostrará un mapa de la región en análisis (Pensilvania), proporcionando un contexto geográfico para las evaluaciones de los restaurantes.

3. **Tablero principal**
   - Tablero principal que presentará la información más relevante del análisis general.
   - Incluirá filtros para facilitar la exploración de datos, permitiendo al usuario seleccionar datos por año y plataforma (Yelp o Google Maps).

4. **KPI 1 y KPI 2**
   - Tablero dedicado a los primeros dos Key Performance Indicators (KPI).
   - Mostrará si se lograron o no los objetivos planteados y proporcionará gráficos para respaldar y profundizar en la comprensión de estos KPI.

5. **KPI 3 y KPI 4**
   - Tablero similar al anterior, pero enfocado en los KPI 3 y 4.

### Implementación del Dashboard

El dashboard se implementará utilizando PowerBI, aprovechando sus capacidades de visualización de datos y facilidades de interactividad.

Para garantizar una experiencia óptima, se recomienda utilizar el dashboard en navegadores modernos y asegurarse de tener una conexión estable a Internet.



## Metodología
El proyecto seguirá la metodología Scrum para la gestión y desarrollo. El equipo está compuesto por:

- [Davidsson Gonzales](https://www.linkedin.com/in/davidsson-gonzalez-usma-6a7486295/) - Data Engineer
- [Francisco Mejia](https://github.com/pachomejia26) - Data Engineer
- [Diego Gamarra Rivera](https://www.linkedin.com/in/diegogamarrarivera/) - Data Scientist
- [Juan Ochoa](https://www.linkedin.com/in/juan-gabriel-ochoa-g/) - Data Scientist
- [Lucas Koch](https://www.linkedin.com/in/lucas-gkoch/) - Data Analyst

## Cloud - Guía General
![Pipeline de datos](https://drive.google.com/uc?id=1YAyKSca3QadvQL0N1CAeAkrrxJLHBHqa)

1. Registrarse en Google Cloud Platform. 
2. Activar facturación y regalo de 300 créditos gratis. (Sólo para quien va a trabajar montando el ecosistema, ya que este beneficio tiene una duración limitada de 3 meses).
    - [Cómo usar Google Cloud Storage](https://www.youtube.com/watch?v=HSIyOin5paQ)
3. Se establece un presupuesto para el proyecto.
4. Asignación de roles y permisos para los demás miembros.
    - [Gestión de Identidades y Accesos de Google Cloud (IAM)](https://www.youtube.com/watch?v=ZS3qyD_cveY)
5. Se crea un bucket en gcp con la herramienta Google Cloud Storage.
    - Se crea el bucket.
    - Se crean las respectivas carpetas de los datasets dentro del bucket.
    - Se suben los archivos en la carpeta datasets.
    - [Cómo usar Google Cloud Storage](https://www.youtube.com/watch?v=HSIyOin5paQ)
6. Se crea un clúster de Apache Spark con la herramienta Dataproc.
    - Se habilitan las API’s de Cloud Resource Manager API y Cloud Dataproc API.
    - Se crea el clúster con Compute Engine.
    - Se suben los scripts de python a la carpeta de un bucket que contienen el código pyspark del ETL.
7. Se crea un conjunto de Datos en BigQuery
    - Se habilitan API’s Google BigQuery
    - Se crea un conjunto de datos
    - Se crea una consulta que crea las tablas de los datos a entrar del ETL
8. Se crean las Cloud Functions
    - Se crea una cloud function para que llene las tablas creadas en BigQuery con los archivos parquet que llegan a un bucket en particular. Esta cloud function se activa con cualquier evento de carga de archivos al bucket.
    - [Introducción a Google Cloud Functions](https://www.youtube.com/watch?v=Ggec25RDy2o)
9. Se ejecutan los trabajos en Dataproc
    - Se realizan de manera manual los trabajos en el clúster creado realizando un llamado a los scripts de ETL
    - La ejecución del ETL guarda los  DataFrames en archivos parquet en un bucket de salida
10. Se ejecuta la Cloud Function de manera automática
    - Al detectar los archivos en el bucket de salida de salida los utiliza para llenar las tablas con sus respectivos nombres asignados

## Análisis Exploratorio de Datos (EDA)
- Yelp: 5 tablas (3 hechos, 2 dimensionales). Datos detallados.
- Google Maps: 2 tablas relacionadas. Menos detallado y con datos faltantes.
- Yelp como fuente principal; Google Maps, complementario según utilidad.

## Archivos complementarios
Ser obtuvo un archivo GeoJson del estado de Pensilvania y sus condados. Este archivo fue descargado del sitio Pennsylvania Spatial Data Access (https://www.pasda.psu.edu/)

## Modelo de Recomendación de Machine Learning
- Ofrece sugerencias personalizadas basadas en gustos y preferencias.
- Incluye tipo de comida, precio, ubicación, etc.

## Informe de Sugerencias para Restaurantes
- Brinda informes a restaurantes sobre áreas de mejora basadas en reseñas y tips de usuarios.
