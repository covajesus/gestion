import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from sklearn.linear_model import LinearRegression
import calendar


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
def QRY_INGRESOS_TOTALES_PBI():
    engine = connect_to_db()
    query = """SELECT
    QRY_INGRESOS_TOTALES_PBI.date,
    QRY_INGRESOS_TOTALES_PBI.branch_office_id,
    QRY_BRANCH_OFFICES.branch_office,
    SUM(QRY_INGRESOS_TOTALES_PBI.abonados+ QRY_INGRESOS_TOTALES_PBI.net_amount+ QRY_INGRESOS_TOTALES_PBI.transbank) as ventas,
    MONTH(QRY_INGRESOS_TOTALES_PBI.date) as month,
    YEAR(QRY_INGRESOS_TOTALES_PBI.date) as `year`
    FROM QRY_INGRESOS_TOTALES_PBI LEFT JOIN QRY_BRANCH_OFFICES
    ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
    WHERE
    YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((YEAR(curdate())-1))  AND QRY_BRANCH_OFFICES.status_id = 15 
    GROUP BY
    QRY_INGRESOS_TOTALES_PBI.date,
    QRY_INGRESOS_TOTALES_PBI.branch_office_id,
    month,
    `year`"""
    # especificamos que la columna 'date' debe ser parseada como fecha
    df_historico = pd.read_sql(query, engine, parse_dates=['date'])
    return df_historico



df_historico = QRY_INGRESOS_TOTALES_PBI()

# Agrupa los datos por mes, día y sucursal y calcula la suma de las ventas
df_historico = df_historico.groupby(['year','month', 'date', 'branch_office_id', 'branch_office'])['ventas'].sum().reset_index()

# Crea una lista con los id de sucursal únicos
branch_offices = df_historico['branch_office'].unique()
selected_branch = st.sidebar.selectbox('Selecciona una sucursal', branch_offices)
# Filtra los datos históricos según la sucursal seleccionada
df_filtered = df_historico[df_historico['branch_office'] == selected_branch]
# Asigna año para proyección 
selected_year = 2024
# Crea un filtro de mes en el sidebar
selected_month = st.sidebar.selectbox('Selecciona un mes', df_filtered['month'].unique())


# Filtra los datos históricos según el año y el mes seleccionados
df_filtered = df_historico[(df_historico['year'] == selected_year) &
                          (df_historico['month'] == selected_month) &
                          (df_historico['branch_office'] == selected_branch)]

df_filtered = df_filtered.reset_index(drop=True)


df_seleccion = df_historico[(df_historico['branch_office'] == selected_branch) & (df_historico['month'] == selected_month)]
st.dataframe(df_seleccion)


def train_models(df):
    models = {}
    for branch, group in df.groupby(['branch_office']):
        # Agrupa los datos por mes
        group = group.groupby(['month'])
        for month, group in group:
            # Crea variables independientes para el día del mes, el día de la semana, el mes y el año
            group['day_of_month'] = group['date'].dt.day
            group['day_of_week'] = group['date'].dt.dayofweek
            group['month'] = month
            group['year'] = selected_year
            # Crea un DataFrame con las variables independientes y dependientes
            X = group[['day_of_month', 'day_of_week', 'month', 'year']]
            y = group['ventas']
            # Entrena el modelo de regresión lineal
            model = LinearRegression()
            model.fit(X, y)
            # Almacena el modelo en un diccionario con el id de la sucursal, el mes y el año como clave
            models[(branch, month, selected_year)] = model
    return models



def predict_sales(models, date, branch_office, month, year):
    # Comprueba si hay un modelo disponible para la sucursal, el mes y el año
    if (branch_office, month, year) not in models:
        return 0
    # Crea variables independientes para el día del mes, el día de la semana, el mes y el año
    day_of_month = date.day
    day_of_week = date.weekday()
    month = month
    year = year
    # Usa el modelo correspondiente a la sucursal, el mes y el año para realizar la predicción
    model = models[(branch_office, month, year)]
    X = pd.DataFrame({'day_of_month': [day_of_month], 'day_of_week': [day_of_week], 'month': [month], 'year': [year]})
    y_pred = model.predict(X)
    return y_pred[0]


# Entrena los modelos de regresión lineal para cada sucursal y mes
models = train_models(df_historico)
# Obtiene el número de días del mes seleccionado
num_days = calendar.monthrange(selected_year, selected_month)[1]
# Crea un DataFrame vacío para almacenar las proyecciones
df_proyeccion = pd.DataFrame(columns=['date', 'branch_office', 'month', 'ventas'])


# Realiza la proyección de ventas para cada día del mes seleccionado, para la sucursal, mes y año seleccionados
for day in range(1, num_days + 1):
    branch = selected_branch
    month = selected_month
    # Crea una fecha con el día, el mes y el año seleccionados
    date = datetime(selected_year, month, day)
    # Realiza la proyección de ventas
    ventas = predict_sales(models, date, branch, month, selected_year)
    # Redondea el valor de ventas proyección
    ventas_rounded = round(ventas)
    # Agrega la proyección al DataFrame
    df_proyeccion = df_proyeccion.append({'date': date, 'branch_office': branch, 'month': month, 'year': selected_year, 'ventas': ventas_rounded}, ignore_index=True)


# Muestra las proyecciones de ventas en la página principal
st.dataframe(df_proyeccion)




