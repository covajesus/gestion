import streamlit as st
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sqlalchemy import create_engine

# Conexión a la base de datos MySQL
def connect_to_db():
    db_config = {
        'host': '103.72.78.28',
        'user': 'jysparki_jis',
        'password': 'Jis2020!',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

# Carga los datos históricos
def get_sales_data():
    engine = connect_to_db()
    query = """SELECT
        QRY_INGRESOS_TOTALES_PBI.date,
        QRY_INGRESOS_TOTALES_PBI.branch_office_id,
        QRY_BRANCH_OFFICES.branch_office,
        SUM(QRY_INGRESOS_TOTALES_PBI.abonados+ QRY_INGRESOS_TOTALES_PBI.net_amount+ QRY_INGRESOS_TOTALES_PBI.transbank) as ventas,
        MONTH(QRY_INGRESOS_TOTALES_PBI.date) as month,
        DAY(QRY_INGRESOS_TOTALES_PBI.date) as dia,
        YEAR(QRY_INGRESOS_TOTALES_PBI.date) as `year`,
        DAYNAME(QRY_INGRESOS_TOTALES_PBI.date) as day_of_week
    FROM QRY_INGRESOS_TOTALES_PBI LEFT JOIN QRY_BRANCH_OFFICES
        ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
    WHERE
        YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) IN (2022, 2023) AND QRY_BRANCH_OFFICES.status_id = 15
    GROUP BY
        QRY_INGRESOS_TOTALES_PBI.date,
        QRY_INGRESOS_TOTALES_PBI.branch_office_id,
        month,
        `year`,
        day_of_week"""

    sales_data = pd.read_sql(query, engine)
    return sales_data

def train_arima_model(data, predict_year, selected_month):
    # Agrupar datos por año, mes y día
    data_daily = data.groupby(['year', 'month', 'dia'])['ventas'].sum().reset_index()

    # Filtrar datos por el mes seleccionado
    selected_month_data = data_daily[data_daily['month'] == selected_month]

    # Crear la columna 'year_month'
    selected_month_data['year_month'] = selected_month_data['year'].astype(str) + '-' + selected_month_data['month'].astype(str).str.zfill(2)

    # Crear serie de tiempo
    branch_ts = selected_month_data.set_index('year_month')['ventas']

    # Dividir datos en entrenamiento y validación
    train_size = int(len(branch_ts) * 0.8)
    train, test = branch_ts[0:train_size], branch_ts[train_size:]

    # Entrenar modelo SARIMAX con el método de máxima verosimilitud (NM)
    model = SARIMAX(train, order=(5, 1, 2), enforce_stationarity=False, enforce_invertibility=False)
    model_fit = model.fit(method='nm')

    # Predecir valores para el año seleccionado y todos los días del mes
    future_dates = pd.date_range(start=f'{predict_year}-{selected_month:02d}-01', periods=31, freq='D')
    future_index = pd.Series(future_dates).dt.strftime('%Y-%m-%d')
    predictions = model_fit.predict(start=len(train), end=len(train) + len(future_dates) - 1)

    # Crear un DataFrame con las predicciones
    predictions_df = pd.DataFrame({'year_month_day': future_index, 'ventas': predictions})
    predictions_df['year'] = predictions_df['year_month_day'].str[:4]
    predictions_df['month'] = predictions_df['year_month_day'].str[5:7]
    predictions_df['dia'] = pd.to_numeric(predictions_df['year_month_day'].str[8:], errors='coerce').fillna(0).astype(int)

    # Filtrar los datos históricos por el mes seleccionado
    historical_data = selected_month_data[['year', 'dia', 'ventas']]

    # Agregar la columna 'year' al DataFrame historical_data
    historical_data = historical_data.pivot(index='dia', columns='year', values='ventas')

    # Renombrar columnas
    historical_data.columns = ['2022', '2023']

    # Unir los datos históricos con las predicciones
    result = pd.merge(historical_data, predictions_df[['dia', 'year', 'ventas']], on='dia', how='outer'|
    result.fillna(0, inplace=True)

    # Renombrar la columna 'ventas' a '2024'
    result = result.rename(columns={'ventas': '2024'})

    # Agregar la columna 'Proyección' con los valores de la columna '2024'
    result['Proyección'] = result['2024']
    return result


def main():
    st.title("Proyecciones de Ventas")
    # Carga los datos
    sales_data = get_sales_data()

    # Selecciona el mes en el sidebar
    selected_month = st.sidebar.selectbox('Selecciona un mes', range(1, 13))

    # Entrena el modelo ARIMA con los datos filtrados
    predictions_df = train_arima_model(sales_data, 2024, selected_month)

    # Muestra el reporte
    st.write(predictions_df)
if __name__ == "__main__":
    main()



