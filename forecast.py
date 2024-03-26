import streamlit as st
import pandas as pd
import numpy as np
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from skforecast.model_selection import backtesting_forecaster
from sqlalchemy import create_engine

st.markdown("# Predicción de ventas")
st.markdown("---")

def connect_to_db():
    db_config = {
        'host': '103.72.78.28',
        'user': 'jysparki_jis',
        'password': 'Jis2020!',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

def execute_query(query):
    engine = connect_to_db()
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result[['date', 'branch_office_id', 'branch_office', 'ventas', 'month', 'dia', 'year', 'day_of_week']]

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
    YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) IN (2022, 2023) AND QRY_BRANCH_OFFICES.status_id = 15
GROUP BY
    QRY_INGRESOS_TOTALES_PBI.date,
    QRY_INGRESOS_TOTALES_PBI.branch_office_id,
    month,
    `year`,
    day_of_week
"""

df = execute_query(query)
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)



def train_model(df, branch_office_id):
    # Filtra los datos para la sucursal seleccionada
    df_branch = df[df['branch_office_id'] == branch_office_id]

    # Crea el modelo
    forecaster = ForecasterAutoreg()

    # Prepara las variables exógenas
    exog_df = df_branch[['month', 'year', 'day_of_week']]

    # Entrena el modelo con las variables exógenas
    model = forecaster.fit(y=df_branch['ventas'], exogenous=exog_df)

    # Realiza backtesting para evaluar la precisión del modelo
    backtesting = backtesting_forecaster(model, df_branch['ventas'], exogenous=exog_df, initial_train_size=24, step=1, strategy='retrain')

    # Calcula los errores de predicción
    errors = backtesting.errors()
    mape = np.mean(np.abs(errors / df_branch['ventas']))

    return model, mape


def predict_sales(model, branch_office_id, start_date, end_date):
    # Filtra los datos para la sucursal seleccionada
    df_branch = df[df['branch_office_id'] == branch_office_id]
    
    # Crea un dataframe con las fechas para las que se desea predecir las ventas
    prediction_dates = pd.date_range(start_date, end_date, freq='D')
    prediction_df = pd.DataFrame(index=prediction_dates, columns=['branch_office_id', 'month', 'year', 'day_of_week'])
    prediction_df['branch_office_id'] = branch_office_id
    prediction_df['month'] = prediction_dates.month
    prediction_df['year'] = prediction_dates.year
    prediction_df['day_of_week'] = prediction_dates.dayofweek

    # Predice las ventas
    prediction = model.predict(n_periods=len(prediction_dates), regressors=prediction_df)
    # Convierte la predicción a un dataframe
    prediction_df['ventas'] = prediction
    return prediction_df


st.title("Predicción de ventas")

# Crea un diccionario para mapear los IDs de las sucursales con sus nombres correspondientes
branch_offices = df.drop_duplicates(subset=['branch_office_id', 'branch_office'])
branch_office_dict = dict(zip(branch_offices['branch_office_id'], branch_offices['branch_office']))

# Selecciona una sucursal
selected_branch_office = st.selectbox("Selecciona una sucursal", branch_office_dict.items())

# Obtiene el ID de la sucursal seleccionada
branch_office_id = selected_branch_office[0]

#branch_office_id = st.selectbox("Selecciona una sucursal", df['branch_office_id'].unique())
start_date = st.date_input("Selecciona la fecha de inicio", pd.to_datetime('2023-01-01'))
end_date = st.date_input("Selecciona la fecha de fin", pd.to_datetime('2023-12-31'))

if st.button("Predecir ventas"):
    model, mape = train_model(df, branch_office_id)
    prediction_df = predict_sales(model, branch_office_id, start_date, end_date)
    st.write("Errores de predicción (MAPE):", mape)
    st.write("Predicción de ventas:")
    st.write(prediction_df)

st.markdown("---")
st.markdown("Jisparking.com")


