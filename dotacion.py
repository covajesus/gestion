import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import streamlit_shadcn_ui as ui
import requests
import json


# Conexión a la base de datos MySQL
def connect_to_db():
    db_config = {
        'host': '103.72.78.28',
        'user': 'jysparki_jis',
        'password': 'Jis2020!',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

def nomina_trabajadores():
    engine = connect_to_db()
    query = """
    SELECT
    Rut,
    Nombre,
    Sucursal,
    Tipo_Contrato,
    Mes,
    Año,
    CONCAT(LPAD(MONTH(Fecha), 2, '0'), "-", YEAR(Fecha)) as Periodo, 
	'nomina' as Fuente
    FROM
    RRHH_Remuneracion
    WHERE
    item = 'DIAS TRABAJADOS' AND Año = '2023';   
    """
    nomina_trabajadores = pd.read_sql(query, engine)    
    return nomina_trabajadores

df_dotacion = nomina_trabajadores()


BASE_EMPLOYEES = 'https://apijis.com/employees/full_details'


def obtener_employees():
    response = requests.get(BASE_EMPLOYEES)
    if response.status_code == 200:
        data = response.json()["message"]
        return data
    else:
        return []

employees_data = obtener_employees()
df_employees = pd.DataFrame(employees_data)

def main(authenticated=False):
    if not authenticated:
        #st.error("Necesitas autenticarte primero")
        #st.error("Necesitas autenticarte")
        #st.stop()
        raise Exception("No autenticado, Necesitas autenticarte primero")
        #return
    else:
        st.title("INFORME DE DOTACIONES")
        
        st.dataframe(df_dotacion)
        # Filtrar df_employees para mostrar solo las columnas requeridas
        df_employees_filtered = df_employees[["rut", "gender", "pention", "salud"]]
        st.dataframe(df_employees_filtered)
        
        

if __name__ == "__main__":
    main()   
