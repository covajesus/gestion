import streamlit as st
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from sklearn.ensemble import RandomForestRegressor
from sqlalchemy import create_engine
import calendar


st.markdown("# Predicción de ventas")
st.markdown("---")

meses = {
    "Enero": "01",
    "Febrero": "02",
    "Marzo": "03",
    "Abril": "04",
    "Mayo": "05",
    "Junio": "06",
    "Julio": "07",
    "Agosto": "08",
    "Septiembre": "09",
    "Octubre": "10",
    "Noviembre": "11",
    "Diciembre": "12"
}

def connect_to_db():
    db_config = {
        'host': '216.137.190.82',
        'user': 'jysparki_admin',
        'password': 'Admin2024$',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

def load_data(query):
    engine = connect_to_db()
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result[['date', 'branch_office_id', 'ventas', 'mes', 'dia', 'year']]

query = """
    #SET lc_time_names = 'es_ES';
    SELECT
        QRY_INGRESOS_TOTALES_PBI.date,
        QRY_INGRESOS_TOTALES_PBI.branch_office_id,
        QRY_BRANCH_OFFICES.branch_office as sucursal,
        SUM(QRY_INGRESOS_TOTALES_PBI.abonados+ QRY_INGRESOS_TOTALES_PBI.net_amount+ QRY_INGRESOS_TOTALES_PBI.transbank) as ventas,
        MONTH(QRY_INGRESOS_TOTALES_PBI.date) as mes,
        DAY(QRY_INGRESOS_TOTALES_PBI.date) as dia,
        YEAR(QRY_INGRESOS_TOTALES_PBI.date) as `year`
    FROM QRY_INGRESOS_TOTALES_PBI LEFT JOIN QRY_BRANCH_OFFICES
        ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
    WHERE
        YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) >= YEAR(CURDATE()-2)
				AND DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) < DAY(CURDATE())
				AND QRY_BRANCH_OFFICES.status_id = 15
    GROUP BY
        QRY_INGRESOS_TOTALES_PBI.date,
        QRY_INGRESOS_TOTALES_PBI.branch_office_id,
        mes,
        `year`;
    """

df = load_data(query)
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

def real_data(year, month_number):
    query = f"""
        SELECT
            QRY_INGRESOS_TOTALES_PBI.date,
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados+ QRY_INGRESOS_TOTALES_PBI.net_amount+ QRY_INGRESOS_TOTALES_PBI.transbank) as ventas,
            MONTH(QRY_INGRESOS_TOTALES_PBI.date) as mes,
            DAY(QRY_INGRESOS_TOTALES_PBI.date) as dia,
            YEAR(QRY_INGRESOS_TOTALES_PBI.date) as `year`
        FROM QRY_INGRESOS_TOTALES_PBI LEFT JOIN QRY_BRANCH_OFFICES
        ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
        WHERE
            YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = {year} AND
            MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = {month_number} AND
            QRY_BRANCH_OFFICES.status_id = 15
        GROUP BY
            QRY_INGRESOS_TOTALES_PBI.date,
            mes,
            `year`"""
    engine = connect_to_db()
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result[['date', 'ventas']]




def preprocess_data(data):
    # Convierte la columna 'date' a tipo datetime
    data['date'] = pd.to_datetime(data['date'])

    # Crea las columnas 'mes' y 'year' a partir de la columna 'date'
    data['mes'] = data['date'].dt.month
    data['year'] = data['date'].dt.year

    # Reindexa el DataFrame con la columna 'date'
    data.set_index('date', inplace=True, drop=True)

    # Rellena los valores nulos con ceros o utiliza otro método de interpolación
    data.fillna(0, inplace=True)

    # Devuelve el DataFrame preprocesado
    return data



def train_random_forest(processed_data, n_estimators=100, max_depth=10, random_state=42):
    # Crea y entrena el modelo de Bosque Aleatorio
    model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, random_state=random_state)
    model.fit(processed_data.drop('ventas', axis=1), processed_data['ventas'])
    # Devuelve el modelo entrenado
    return model


def predict_random_forest(model, year, month, branch_office_id=None):
    # Crea una lista de fechas para el mes y año seleccionados
    days_in_month = calendar.monthrange(year, month)[1]
    dates = pd.date_range(start=f"{year}-{month}-01", periods=days_in_month, freq="D")

    # Crea un DataFrame con las características correspondientes a las fechas seleccionadas
    X_futuro = pd.DataFrame({
        'branch_office_id': np.nan,  # incluir la columna branch_office_id, aunque todos sus valores sean NaN
        'dia': np.nan,  # incluir la columna dia, aunque todos sus valores sean NaN
        'mes': month,
        'year': year,
        # ... (incluir otras variables relevantes, como el día de la semana, etc.)
    }, index=dates)

    # Reemplazar los valores NaN en X_futuro con un valor apropiado
    X_futuro.fillna(0, inplace=True)

    # Reordenar las columnas de X_futuro para que coincidan con el orden de las columnas en el DataFrame de entrenamiento
    X_futuro = X_futuro[model.feature_names_in_]

    # Si se pasó una branch_office_id específica, filtrar el DataFrame X_futuro para incluir solo esa branch_office_id
    if branch_office_id is not None:
        X_futuro = X_futuro.replace({'branch_office_id': {np.nan: branch_office_id}})

    # Realiza las predicciones con el modelo de Bosque Aleatorio
    predictions = model.predict(X_futuro)

    # Devuelve las predicciones
    return predictions

   





# Carga la data histórica
data = load_data(query)

# Obtener la lista de branch_office_id únicas
branch_offices = data['branch_office_id'].unique()

# Preprocesa la data histórica
processed_data = preprocess_data(data)

# Entrena el modelo de Bosque Aleatorio
model = train_random_forest(processed_data)


# Crea los selectbox en el sidebar
year = st.sidebar.selectbox("Selecciona el año de predicción", [2024])
month = st.sidebar.selectbox("Selecciona el mes de predicción", list(meses.keys()))
month_number = meses[month]

# Agregar un selectbox en el sidebar con las opciones de branch_office_id
selected_branch_office = st.sidebar.selectbox('Selecciona la sucursal:', ['all'] + branch_offices.astype(str).tolist())

if st.sidebar.button("Realizar predicción"):
    # Realiza las predicciones con el modelo de Bosque Aleatorio
    if selected_branch_office != 'all':
        predictions = predict_random_forest(model, year, int(month_number), int(selected_branch_office))
    else:
        predictions = predict_random_forest(model, year, int(month_number))

    # Crea una lista de fechas para el mes y año seleccionados
    days_in_month = calendar.monthrange(year, int(month_number))[1]
    dates = pd.date_range(start=f"{year}-{month_number}-01", periods=days_in_month, freq="D")

    # Crea un dataframe con las predicciones y las fechas
    predictions_df = pd.DataFrame({"Fecha": dates, "Prediccion": predictions})


    # Obtiene los datos reales de ventas para el mes y año seleccionado
    historic_data = real_data(year, int(month_number))

    # Convierte la columna "date" a datetime64[ns] en historic_data
    historic_data['date'] = pd.to_datetime(historic_data['date'])

    # Realiza un merge de los dos dataframes utilizando la columna "Fecha" como clave
    result_df = pd.merge(predictions_df, historic_data, left_on="Fecha", right_on="date", how="left")

    # Renombra la columna "ventas" a "Real"
    result_df.rename(columns={"ventas": "Real"}, inplace=True)

    # Elimina la columna "date" del dataframe resultante, ya que es redundante con "Fecha"
    result_df.drop(columns=["date"], inplace=True)

    # Muestra las predicciones y los datos históricos en Streamlit
    st.write("Predicciones de ventas para", month, year)
    st.write(result_df)
