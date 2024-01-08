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

### Desarrollo del Dashboard
- PowerBI

### Endpoints
- Deployment Streamlit

## Metodología
El proyecto seguirá la metodología Scrum para la gestión y desarrollo. El equipo está compuesto por:

- [Davidsson Gonzales](https://www.linkedin.com/in/davidsson-gonzalez-usma-6a7486295/) - Data Engineer
- [Francisco Mejia](https://github.com/pachomejia26) - Data Engineer
- [Diego Gamarra Rivera](https://www.linkedin.com/in/diegogamarrarivera/) - Data Scientist
- [Juan Ochoa](https://www.linkedin.com/in/juan-gabriel-ochoa-g/) - Data Scientist
- [Lucas Koch](https://www.linkedin.com/in/lucas-gkoch/) - Data Analyst

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


## Análisis Exploratorio de Datos (EDA)
- Yelp: 5 tablas (3 hechos, 2 dimensionales). Datos detallados.
- Google Maps: 2 tablas relacionadas. Menos detallado y con datos faltantes.
- Yelp como fuente principal; Google Maps, complementario según utilidad.

## Modelo de Recomendación de Machine Learning
- Ofrece sugerencias personalizadas basadas en gustos y preferencias.
- Incluye tipo de comida, precio, ubicación, etc.

## Informe de Sugerencias para Restaurantes
- Brinda informes a restaurantes sobre áreas de mejora basadas en reseñas y tips de usuarios.
