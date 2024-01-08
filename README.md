# Yelp - Google Maps

## Objetivos del Proyecto
Prometheus Data Solutions, en colaboración con una asociación de restaurantes en Pennsylvania, ha emprendido un proyecto para evaluar la percepción de los clientes sobre los establecimientos afiliados a través de las reseñas en Yelp y Google Maps. Los objetivos incluyen medir la satisfacción, identificar preferencias, señalar áreas de mejora, destacar sectores con oportunidades de crecimiento y prever aquellos que podrían enfrentar disminuciones en las ventas. Además, se busca diseñar un sistema de recomendación mediante Machine Learning que ofrezca sugerencias personalizadas a los usuarios.

## Alcance del Proyecto
El proyecto abarcará el proceso ETL y el Análisis Exploratorio de Datos (EDA), considerando la integración de información proveniente de Yelp y Google Maps. Se utilizarán herramientas como PySpark y Streamlit. El modelo de recomendación de Machine Learning incorporará análisis de sentimientos, técnicas de procesamiento de lenguaje natural (NLP) y considerará restaurantes de grandes cadenas en otros estados para mejorar la percepción en Pennsylvania.

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

### Desarrollo del Dashboard
- PowerBI

### Endpoints
- Deployment Streamlit

## Metodología
El proyecto seguirá la metodología Scrum para la gestión y desarrollo. El equipo está compuesto por:

- **Davidsson Gonzales (Data Engineer)**
- **Francisco Mejia (Data Engineer)**
- **Diego Gamarra Rivera (Data Scientist)**
- **Juan Ochoa (Data Scientist)**
- **Lucas Koch (Data Analyst

## Cloud - Guía General
1. **Crear una cuenta de GCP:** Visita la página de GCP y sigue el proceso de inscripción.
2. **Habilitar la facturación:** Activa la facturación para acceder a los servicios.
3. **Crear un proyecto:** Agrupa los recursos de GCP en un proyecto.
4. **Subir tus datos:** Utiliza Cloud Storage para almacenar conjuntos de datos.
5. **Proceso de ETL:** Emplea Dataproc para el procesamiento de datos en clústeres.
6. **Crear un Data Warehouse:** Utiliza BigQuery para almacenar y analizar grandes conjuntos de datos.
7. **Vincular tu Data Warehouse con Notebooks:** Conecta tu Data Warehouse con Dataproc.
8. **Realizar EDA:** Utiliza Dataproc para explorar patrones y relaciones clave.
9. **Crear un Dashboard:** Utiliza Cloud Data Studio para crear paneles interactivos.
10. **Entrenar un Modelo de Machine Learning:** Emplea Cloud AutoML para crear modelos sin experiencia previa en ML.

## Recomendaciones Adicionales
- Familiarízate con conc
