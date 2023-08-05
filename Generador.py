import requests
import pandas as pd

"""Realizamos el get del API JSON con un codigo en caso de error, en el 
proceso ETL este es el inicio de la extraccion"""
def get_character_data():
    url = "https://rickandmortyapi.com/api/character"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error al obtener los datos. Código de respuesta: {response.status_code}")
        return None
    
    
"""Primero vamos a determinar que keys o atributos tienen los datos dentro del JSON
para poder generar un DataFrame con los datos analizados, esto tambien nos ayudara a 
determinar que datos pueden o no sernos de utilidad"""
def keys(data):
    if data and "results" in data and len(data["results"]) > 0:
        first_character = data["results"][0]
        keys = list(first_character.keys())
        return keys
    else:
        print("Error en los datos")
        return None
    

"""En esta parte se delimitan todas las condiciones de los datos
en funcion de lo que consideramos mas util para los nuestra resultado final, en esta parte hacemos la 
limpieza y acondicionamiento"""

def extract(data):
    if data and "results" in data and len(data["results"]) > 0:
        #Se crea el DataFrame jalando todo de los "result" en el JSON
        df = pd.DataFrame(data["results"])
        
        # Rellenar automáticamente con "S/N" en la columna "type" si está vacío para evitar valores faltantes 
        df['type'] = df['type'].apply(lambda x: x if x else "S/N")

        # Reemplazar "unknown" con "desconocido" en la columna "gender"
        df['gender'] = df['gender'].replace('unknown', 'Desconocido')

        # Reemplazar "unknown" con "desconocido" en la columna "status"
        df['status'] = df['status'].replace('unknown', 'Desconocido')

        # Cuenta el número de elementos en la columna "episode" y crear una nueva columna "num_episodes" ya que esto sera de mayor utilidad
        df['num_episodes'] = df['episode'].apply(len)

        #En esta parte solo extraemos el sub atributo "name" del atributo "origin", se considera que sera mas util
        df['origin_name'] = df['origin'].apply(lambda x: x['name'])

         # Reemplazar "unknown" con "desconocido" en la columna "origin_name"
        df['origin_name'] = df['origin_name'].replace('unknown', 'Desconocido')

        # Eliminar la columna original "episode" asi como las que no sera de utilidad por su naturaleza de poca estadistica
        columns_to_drop = ["origin","location", "url", "created","image"]
        df = df.drop(columns=columns_to_drop)
        df.drop(columns=['episode'], inplace=True)

        # Renombrar algunas columnas para que tengan nombres más amigables y faciles de leer
        df = df.rename(columns={"id": "ID", "name": "Nombre", "status": "Estado", "species": "Especie",
                                "type": "Tipo", "gender": "Género", "num_episodes": "Num de Episodios","origin_name":"Lugar de Origen"})

        return df
    else:
        print("Error en los datos")
        return None
    



if __name__ == "__main__":
    #Aqui se impren todos los keys disponibles solo para evaluar su utilidad
    character_data = get_character_data()
    if character_data:
        keys = keys(character_data)
        if keys:
            print("Los keys disponibles son:")
            print(keys)
    #Se llama la funcion con condiciones y se impren solo los 5 primeros objetos para previsualizarlo en consola 
    df = extract(character_data)
    if df is not None:
            print("Previsualizacion de DataFrame:")
            print(df.head())
             # Exportar el DataFrame a un archivo Excel
            nombre_archivo = "personajes_rick_and_morty.xlsx"
            df.to_excel(nombre_archivo, index=False)

            print("DataFrame exportado en formato Excel.")