import streamlit as st
import pandas as pd
import numpy as np
import calendar
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
from sqlalchemy import create_engine
import datetime

# Conexión a la base de datos MySQL
def connect_to_db():
    db_config = {
        'host': '216.137.190.82',
        'user': 'jysparki_admin',
        'password': 'Admin2024$',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

# Carga los datos históricos
def get_sales_data():
    engine = connect_to_db()
    query = """
    SELECT
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
        YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) IN ((YEAR(curdate())-2), (YEAR(curdate())-1)) AND QRY_BRANCH_OFFICES.status_id = 15
    GROUP BY
        QRY_INGRESOS_TOTALES_PBI.date,
        QRY_INGRESOS_TOTALES_PBI.branch_office_id,
        month,
        `year`,
        day_of_week"""
   
    sales_data = pd.read_sql(query, engine)
    return sales_data

def train_arima_model(data, branch_id, predict_year, selected_month):
    # Filtrar datos por sucursal y agrupar por año y mes
    branch_data = data[data['branch_office_id'] == branch_id]
    branch_data_monthly = branch_data.groupby(['year', 'month'])['ventas'].sum().reset_index()

    # Crear la columna 'year_month'
    branch_data_monthly['year_month'] = branch_data_monthly['year'].astype(str) + '-' + branch_data_monthly['month'].astype(str).str.zfill(2)

    # Crear serie de tiempo
    branch_ts = branch_data_monthly.set_index('year_month')['ventas']

    # Dividir datos en entrenamiento y validación
    train_size = int(len(branch_ts) * 0.8)
    train, test = branch_ts[0:train_size], branch_ts[train_size:]

    # Entrenar modelo SARIMAX con el método de máxima verosimilitud (NM)
    model = SARIMAX(train, order=(5, 1, 2), enforce_stationarity=False, enforce_invertibility=False)
    model_fit = model.fit(method='nm')

    # Predecir valores para el año 2024 y el mes seleccionado
    future_dates = pd.date_range(start=f'{predict_year}-{selected_month:02d}-01', periods=1, freq='MS')
    future_index = pd.Series(future_dates).dt.strftime('%Y-%m')
    predictions = model_fit.predict(start=len(train), end=len(train) + len(future_dates) - 1)

     # Crear un DataFrame con las predicciones
    predictions_df = pd.DataFrame({'year_month': future_index, 'ventas': predictions})
    predictions_df['year'] = predictions_df['year_month'].str[:4]
    predictions_df['month'] = predictions_df['year_month'].str[5:]
    predictions_df = predictions_df[['year', 'month', 'ventas']]

    return predictions_df

def get_real_data(sales_data, selected_branch_id, selected_month, selected_year):
    real_data = sales_data[(sales_data['branch_office_id'] == selected_branch_id) &
                           (sales_data['month'] == selected_month) &
                           (sales_data['year'] == selected_year)]
    real_data['ventas'] = pd.to_numeric(real_data['ventas'], errors='coerce')
    real_data = real_data.groupby(['year', 'month', 'dia'])['ventas'].sum().reset_index()

    # Agregar la columna 'date' a partir de 'year', 'month' y 'dia'
    real_data['date'] = pd.to_datetime({'year': real_data['year'], 'month': real_data['month'], 'day': real_data['dia']})
    return real_data


def main():
    st.title("Proyección de Ventas por Sucursal y Mes para 2024")

    sales_data = get_sales_data()
    
    # Crear un DataFrame auxiliar con las columnas 'branch_office_id' y 'branch_office'
    branch_data = sales_data[['branch_office_id', 'branch_office']].drop_duplicates()

    # Mostrar selección de sucursal
    branch_options = branch_data['branch_office'].unique()
    branch_options = ['Todas las sucursales'] + list(branch_options)
    selected_branch_name = st.selectbox("Seleccione una sucursal", branch_options)

    # Selección de mes
    month_options = range(1, 13)
    selected_month = st.selectbox("Seleccione un mes", month_options)

    if selected_branch_name != 'Todas las sucursales':
        # Obtener el ID de la sucursal seleccionada
        selected_branch_id = branch_data.loc[branch_data['branch_office'] == selected_branch_name, 'branch_office_id'].iloc[0]
    else:
        selected_branch_id = None

    if st.button("Realizar Proyección"):
        if selected_branch_id is not None:
            # Obtener los datos reales del mes y sucursal seleccionados hasta el día de ayer
            real_data = get_real_data(sales_data, selected_branch_id, selected_month, 2024)
            last_day_of_real_data = real_data['dia'].max() if not real_data.empty else 0

            # Obtener las predicciones para el mes y sucursal seleccionados
            predictions_df = train_arima_model(sales_data, selected_branch_id, 2024, selected_month)

            # Crear un DataFrame con los días del mes seleccionado
            days_in_month = calendar.monthrange(2024, selected_month)[1]
            dates_index = pd.date_range(start=f'2024-{selected_month:02d}-01', end=f'2024-{selected_month:02d}-{days_in_month}', freq='D')
            days_df = pd.DataFrame({'date': dates_index})
            days_df['year'] = 2024
            days_df['month'] = selected_month
            days_df['dia'] = days_df['date'].dt.day

            # Rellenar los valores faltantes de la columna 'Proyección' con ceros
            real_data = real_data.fillna({'ventas': 0})
            predictions_df = predictions_df.fillna({'Proyección': 0})

            # Combinar los datos reales y las predicciones
            combined_df = pd.concat([real_data, predictions_df], ignore_index=True)
            combined_df = combined_df.set_index(['year', 'month', 'dia'])

            # Agregar la columna 'dia' a combined_df
            combined_df.reset_index(inplace=True)

            # Rellenar los datos faltantes con ceros
            days_index = pd.MultiIndex.from_arrays([days_df['year'], days_df['month'], days_df['dia']], names=['year', 'month', 'dia'])
            combined_df = combined_df.reindex(days_index, fill_value=0)

            # Renombrar la columna 'ventas' a 'Proyección' si es necesario
            if 'ventas' in combined_df.columns:
                combined_df = combined_df.rename(columns={'ventas': 'Proyección'})

            # Reemplazar los valores proyectados por los datos reales hasta la fecha de hoy
            combined_df.loc[(combined_df['year'] == 2024) &
                (combined_df['month'] == selected_month) &
                (combined_df['dia'] <= last_day_of_real_data), 'Proyección'] = real_data.loc[real_data['dia'] <= last_day_of_real_data, 'ventas'].values

            # Obtener los datos históricos para 2022 y 2023
            historical_data = sales_data[(sales_data['branch_office_id'] == selected_branch_id) &
                                         (sales_data['month'] == selected_month) &
                                         ((sales_data['year'] == 2022) | (sales_data['year'] == 2023))]
            historical_data = historical_data.groupby(['year', 'month', 'dia'])['ventas'].sum().reset_index()
            historical_data = historical_data.pivot(index='dia', columns='year', values='ventas').fillna(0)

            # Combinar los datos proyectados y los históricos en un solo DataFrame
            final_df = combined_df.join(historical_data, how='left')
            final_df = final_df.rename(columns={2022: 2022, 2023: 2023, 2024: 'Proyección'})
            final_df = final_df[['Proyección', 2022, 2023]].reset_index()
            final_df = final_df.rename(columns={'dia': 'Día'})          
            
            # Cambiar el formato de las columnas numéricas para mostrar el símbolo de moneda y separadores de miles
            final_df['Proyección'] = pd.to_numeric(final_df['Proyección'], errors='coerce')
            final_df['Proyección'] = final_df['Proyección'].apply(lambda x: '${:,.0f}'.format(x) if pd.notna(x) else '')
            final_df[2022] = final_df[2022].apply(lambda x: '${:,.0f}'.format(x) if pd.notna(x) else '')
            final_df[2023] = final_df[2023].apply(lambda x: '${:,.0f}'.format(x) if pd.notna(x) else '')


            # Convertir la columna 'Proyección' a tipo numérico
            final_df['Proyección'] = pd.to_numeric(final_df['Proyección'], errors='coerce')

            # Calcular el total
            total_proyeccion = final_df['Proyección'].sum()
            total_2022 = final_df[2022].sum()
            total_2023 = final_df[2023].sum()

            # Agregar una fila al final con los totales
            total_row = pd.DataFrame({'Día': ['Total'], 'Proyección': [total_proyeccion], 2022: [total_2022], 2023: [total_2023]})
            final_df = pd.concat([final_df, total_row], ignore_index=True)

         
            # Formatear la fila de totales
            final_df.loc[final_df['Día'] == 'Total', 'Proyección'] = '**${:,.0f}**'.format(total_proyeccion)
            final_df.loc[final_df['Día'] == 'Total', 2022] = total_2022  # <-- Aquí está la línea corregida
            final_df.loc[final_df['Día'] == 'Total', 2023] = total_2023  # <-- También aquí

            # Mostrar los resultados finales
            st.subheader(f"Proyecciones de Ventas para la Sucursal {selected_branch_name} en el Mes {selected_month} de 2024")
            st.table(final_df)

        else:
            all_branches_predictions = []
            for branch_id, branch_name in zip(branch_data['branch_office_id'], branch_data['branch_office']):
                predictions_df = train_arima_model(sales_data, branch_id, 2024, selected_month)
                predictions_df['branch_office'] = branch_name
                all_branches_predictions.append(predictions_df)

            all_branches_predictions_df = pd.concat(all_branches_predictions, ignore_index=True)
            st.subheader(f"Proyecciones de Ventas para Todas las Sucursales en el Mes {selected_month} de 2024")
            st.write(all_branches_predictions_df)

if __name__ == "__main__":
    main()
