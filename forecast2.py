import streamlit as st
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import statsmodels.api as sm

# Cargamos la base de datos
df = pd.read_csv('historico.csv', encoding='unicode_escape')

# Preparación de los datos
# Convertir variables categóricas a numéricas (one-hot encoding)
df = pd.get_dummies(df, columns=['dia_semana', 'sucursal'])

# Convertir la columna 'fecha' a formato de fecha de pandas
df['date'] = pd.to_datetime(df['date'])

# Crear una nueva columna 'epoca' con el número de días transcurridos desde una fecha de referencia
df['epoca'] = (df['date'] - pd.Timestamp('2020-01-01')) / np.timedelta64(1, 'D')

# Seleccionar las columnas de interés (X e y)
X = df.drop(['ventas'], axis=1)
y = df['ventas']

# Eliminar filas con valores faltantes
X, y = X.dropna(), y.dropna()

# División del conjunto de datos
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Selección de modelos
modelos = {
    'ARIMA': None,
    'Regresión Lineal': LinearRegression(),
    'Árbol de Decisión': DecisionTreeRegressor(),
    'Bosque Aleatorio': RandomForestRegressor(),
    'Gradiente Boosting': GradientBoostingRegressor()
}

# Entrenamiento de los modelos
for nombre, modelo in modelos.items():
    if modelo is not None:
        modelo.fit(X_train, y_train)
        modelos[nombre] = modelo

# Evaluación de los modelos
for nombre, modelo in modelos.items():
    if modelo is not None:
        y_pred = modelo.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        st.write(f'{nombre}: MSE = {mse}, MAE = {mae}, R² = {r2}')

# Selección del mejor modelo
mejor_modelo = max(modelos.items(), key=lambda x: r2_score(y_test, x[1].predict(X_test)))

# Crea los selectbox en el sidebar
year = st.sidebar.selectbox("Selecciona el año de predicción", [2024])
month = st.sidebar.selectbox("Selecciona el mes de predicción", list(meses.keys()))
month_number = meses[month]

if st.sidebar.button("Realizar predicción"):
    # Crea una lista de fechas para el mes y año seleccionados
    days_in_month = calendar.monthrange(year, int(month_number))[1]
    dates = pd.date_range(start=f"{year}-{month_number}-01", periods=days_in_month, freq="D")

    # Crea un DataFrame con las características correspondientes a las fechas seleccionadas
    X_futuro = pd.DataFrame({
        'date': dates,
        'epoca': (dates - pd.Timestamp('2020-01-01')) / np.timedelta64(1, 'D'),
        'year': year,
        'month': int(month_number),
        'day': dates.day,
        'day_of_week': dates.dt.dayofweek,
        'day_of_year': dates.dt.dayofyear,
        'is_month_start': (dates.dt.day == 1).astype(int),
        'is_month_end': (dates.dt.day == days_in_month).astype(int),
        'is_year_start': (dates.dt.day == 1).astype(int),
        'is_year_end': (dates.dt.day == 365).astype(int)
    })

    # Convertir variables categóricas a numéricas (one-hot encoding)
    X_futuro = pd.get_dummies(X_futuro, columns=['day_of_week'])

    # Realizar las proyecciones con el mejor modelo
    y_futuro = mejor_modelo[1].predict(X_futuro)

    # Crear un DataFrame con las proyecciones
    proyecciones = pd.DataFrame({
        'dia': X_futuro['date'],
        'mes': X_futuro['month'],
        'año': X_futuro['year'],
        'ventas_proyectadas': y_futuro
    })

    # Ordenar proyecciones por día, mes y año
    proyecciones = proyecciones.sort_values(['año', 'mes', 'dia'])

    # Mostrar las proyecciones
    st.dataframe(proyecciones)