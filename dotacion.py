import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests

BASE_EMPLOYEES = 'https://apijis.azurewebsites.net/employees/full_details'


def obtener_employees():
    response = requests.get(BASE_EMPLOYEES)
    if response.status_code == 200:
        data = response.json()["message"]
        return data
    else:
        return []

employees_data = obtener_employees()
df_employees = pd.DataFrame(employees_data)

def main():
    st.title("INFORME DE DOTACIONES")
    # Filtrar df_employees para mostrar solo las columnas requeridas
    df_employees_filtered = df_employees[["visual_rut", "nickname" , "nationality", "contract_type" , "job_position" , "gender", "pention", "health", "branch_office_id", "branch_office", "born_date", "region"]]
    st.dataframe(df_employees_filtered)
        
        

if __name__ == "__main__":
    main()   
