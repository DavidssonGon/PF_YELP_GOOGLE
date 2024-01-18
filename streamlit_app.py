import streamlit as st
import requests
import pandas as pd
import folium
from streamlit.components.v1 import html



# Mapeo de nombres visuales a valores internos
atributos_mapping = {
    "Cervezas y vinos": "BeerAndWine",
    "Full Bar": "FullBar",
    "Cocheras": "Garage",
    "Apto para niños": "GoodForKids",
    "TV": "HasTV",
    "Estacionamiento": "Lot",
    "Espacios al aire libre": "OutdoorSeating",
    "Delivery": "RestaurantsDelivery",
    "Bueno para grupos": "RestaurantsGoodForGroups",
    "Reservas": "RestaurantsReservations",
    "Comida para llevar": "RestaurantsTakeOut",
    "Valet parking": "Valet"
}

# Mapeo de nombres visuales a valores internos para la opción "Ordenar por"
ordenar_por_mapping = {
    "Cercanía": "distance",
    "Análisis de sentimiento de los clientes": "nlp"
}

# Función para obtener la ubicación del usuario a través de la API de Google
def get_location():
    # Clave de la API de Google Maps
    api_key = 'AIzaSyAf2hDafHt6-qes9aPKlpYk2xfblrfvlso'

    #URL de la API de Geolocalización de Google Maps
    url = f'https://www.googleapis.com/geolocation/v1/geolocate?key={api_key}'
    response = requests.post(url)

    if response.status_code == 200:
        location_data = response.json()
        latitud = location_data['location']['lat']
        longitud = location_data['location']['lng']
        return {'latitud': latitud, 'longitud': longitud}
    else:
        print(f'Error al obtener la ubicación. Código de respuesta: {response.status_code}')
        return {}


# Crear una función para la recomendación de restaurantes basada en los atributos seleccionados
def recomendar_restaurante(correo_usuario, atributos_seleccionados, acepta_tarjeta, negocio_validado, ordenar_por, radio_busqueda_km, latitud_manual, longitud_manual):
    # Mapear nombres visuales a valores internos y ordenar alfabéticamente
    atributos_seleccionados_internos = sorted([atributos_mapping[atributo] for atributo in atributos_seleccionados])

    # Mapear nombres visuales a valores internos para la opción "Ordenar por"
    ordenar_por_interno = ordenar_por_mapping.get(ordenar_por, ordenar_por)

    #Se crea el diccionario que se va a enviar
    recomendacion = {
        'Business_Id': 'solicitado',
        'Name': correo_usuario,
        'Address': 'usuario',
        'Latitude': latitud_manual,
        'Longitude': longitud_manual,
        'BusinessAcceptsCreditCards': acepta_tarjeta,
        'Validated': negocio_validado,
        'AllAttributes': ' '.join(atributos_seleccionados_internos),
        'Radius': radio_busqueda_km,
        'OrderBy': ordenar_por_interno}
    
    print(recomendacion)
    
    cloud_function_url = "https://us-central1-prometheus-data-solutions.cloudfunctions.net/streamlit_app"
    response = requests.post(cloud_function_url, json=recomendacion)

        # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        # Obtener el JSON de la respuesta
        json_recomendados = response.json()["df_recomendados"]

        # Convertir el JSON a DataFrame de Pandas
        df_recomendados = pd.read_json(json_recomendados, orient='records')

        if not df_recomendados.empty:

            # Mostrar el DataFrame en Streamlit
            st.dataframe(df_recomendados[['Restaurante','Dirección','Distancia(km)','Análisis de sentimiento']])

            # Crear un mapa de folium centrado en Connecticut
            map = folium.Map(location=[latitud_manual,longitud_manual], zoom_start=10)

            # Agregar marcadores para cada restaurante recomendado
            for index, row in df_recomendados.iterrows():
                location = (row['Latitud'], row['Longitud'])
                folium.Marker(location, popup=row['Restaurante']).add_to(map)

            # Agregar marcador para la ubicación del usuario actual con un ícono diferente y color
            folium.Marker([latitud_manual, longitud_manual], popup='Tu ubicación', icon=folium.Icon(color='red')).add_to(map)

            # Convertir el mapa de Folium a HTML
            map_html = folium.Figure().add_child(map)._repr_html_()

            # Mostrar el HTML en la interfaz de Streamlit
            st.markdown('Ubicación de los restaurantes recomendados:')
            html(map_html, width=700, height=500)
        else:
            # Mostrar un mensaje si el DataFrame está vacío
            st.warning('No se encontraron restaurantes adecuados en el radio de búsqueda.')

    else:
        st.error("Error al obtener datos de la Cloud Function")



# Configuración de la aplicación
st.set_page_config(page_title="Recomendador de Restaurantes", page_icon="🍔")

# Inicializar la sesión del estado
if 'ubicacion_usuario' not in st.session_state:
    st.session_state.ubicacion_usuario = {'latitud': None, 'longitud': None}

# Configuración de la aplicación
st.title("Recomendador de Restaurantes")


# Obtener el correo electrónico del usuario (opcional)
correo_usuario = st.text_input("Ingrese su correo electrónico:")


# Botón para obtener la ubicación
st.markdown("Haga click en el boton para obtener su ubicación")
obtener_ubicacion_btn = st.button("Obtener Ubicación", key="obtener_ubicacion_btn")
# Lógica para obtener la ubicación cuando se presiona el botón
if obtener_ubicacion_btn:
    # Llamando a la función y almacenando el resultado en una variable
    ubicacion_dict = get_location()
    
    # Accediendo a la latitud y longitud desde el diccionario
    latitud = ubicacion_dict.get('latitud')
    longitud = ubicacion_dict.get('longitud')

    # Mostrando la información en pantalla
    st.write(f'Ubicación obtenida a través de la API: Latitud {latitud}, Longitud {longitud}')

# Dos campos para ingresar manualmente la latitud y la longitud
latitud_manual = st.number_input("Latitud (Manual)", min_value=-90.0, max_value=90.0, value=0.0)
longitud_manual = st.number_input("Longitud (Manual)", min_value=-180.0, max_value=180.0, value=0.0)

st.markdown("Indique si alguno de los siguientes aspectos es obligatorio:")
acepta_tarjeta = st.checkbox("Acepta tarjeta de crédito")
negocio_validado = st.checkbox("Negocio validado por Yelp")

# Lista de atributos para seleccionar
atributos_disponibles = [
    "Cervezas y vinos", "Full Bar", "Cocheras", "Apto para niños",
    "TV", "Estacionamiento", "Espacios al aire libre", "Delivery", "Bueno para grupos",
    "Reservas", "Comida para llevar", "Valet parking"
]

# Selección de atributos por parte del usuario

atributos_seleccionados = st.multiselect("Seleccione los atributos que le gustan de un restaurante:", atributos_disponibles)

# Criterios de ordenación

ordenar_por = st.radio("Ordenar por:", ["Cercanía", "Análisis de sentimiento de los clientes"])

# Radio de búsqueda
radio_busqueda_km = st.selectbox("Radio de búsqueda(km):", [5, 10, 15, 20])

# Botón para realizar la recomendación
if st.button("Obtener Recomendación"):
    # Validar si se ingresaron requisitos estrictos y atributos seleccionados
    if  atributos_seleccionados:
        # Mostrar la recomendación al usuario
        recomendacion = recomendar_restaurante(correo_usuario, atributos_seleccionados, acepta_tarjeta, negocio_validado, ordenar_por, radio_busqueda_km, latitud_manual,longitud_manual)
        # Mostrar la recomendación al usuario
        st.write(recomendacion)
    else:
        st.warning("Por favor, seleccione los requisitos estrictos y al menos un atributo.")


