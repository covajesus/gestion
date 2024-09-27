import streamlit as st
import pandas as pd
import requests
import plotly.express as px


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

    # Agrupar datos y contar ocurrencias
    nationality_count = df_employees["nationality"].value_counts()
    gender_count = df_employees["gender"].value_counts()
    pention_count = df_employees["pention"].value_counts()
    health_count = df_employees["health"].value_counts()

    # Calcular totales
    nationality_total = nationality_count.sum()
    gender_total = gender_count.sum()
    pention_total = pention_count.sum()
    health_total = health_count.sum()

    # Agregar totales a los DataFrames
    nationalityes = pd.concat([nationality_count, pd.Series(nationality_total, index=["Total"])])
    gender_count = pd.concat([gender_count, pd.Series(gender_total, index=["Total"])])
    pention_count = pd.concat([pention_count, pd.Series(pention_total, index=["Total"])])
    health_count = pd.concat([health_count, pd.Series(health_total, index=["Total"])])

    col1, col2 = st.columns(2)
    with col1:
            st.subheader("Nacionalidades")
            st.write(nationalityes)  
            
    with col2:
            fig = px.pie(names=nationality_count.index, values=nationality_count.values, color_discrete_sequence=px.colors.sequential.RdBu, title="Distribución")
            st.plotly_chart(fig, theme="streamlit")

    st.subheader("Cantidad de géneros:")
    st.write(gender_count)

    st.subheader("Cantidad de tipos de pensión:")
    st.write(pention_count)

    st.subheader("Cantidad de tipos de salud:")
    st.write(health_count)

if __name__ == "__main__":
    main()    
