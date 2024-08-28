import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import mysql.connector
from mysql.connector import Error

def get_db_connection():
    try:
        connection_params = {
            'host': '216.137.190.82',
            'user': 'jysparki_admin',
            'password': 'Admin2024$',
            'database': 'jysparki_jis',
            'charset': 'utf8',
            'collation': 'utf8_general_ci',
        }
        conn = mysql.connector.connect(**connection_params)
        return conn
    except Error as e:
        st.write(f"The error '{e}' occurred")

#Informe de ventas - Acumulados       

def update_kpi_ingresos_acumulado_act(conn):
    try:        
        query = "DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2024' AND Periodo = 'Acumulado' and metrica = 'ingresos'"
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        query = """
        INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
        SELECT
         'Acumulado' as Periodo,
         DM_PERIODO.period,
         DM_PERIODO.`Año`,
         QRY_BRANCH_OFFICES.branch_office,
         QRY_IND_SSS.`value`,
         SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number,
         SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados,
         SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount,
         SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,
         SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta,
         SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos,
         '0' as ppto,
         SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS,
         SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
         'ingresos' as metrica
        FROM QRY_INGRESOS_TOTALES_PBI
        LEFT JOIN QRY_BRANCH_OFFICES
        ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
        LEFT JOIN QRY_IND_SSS
        ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
        LEFT JOIN DM_PERIODO
        ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
        WHERE
            QRY_BRANCH_OFFICES.status_id = 15 AND
            DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) < (DAY(CURDATE())) AND
            MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate()))) AND
            YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())
        GROUP BY
            DM_PERIODO.Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office
        ORDER BY
            QRY_INGRESOS_TOTALES_PBI.date ASC
        """
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        st.write("Ingresos Actual Acumulado, Cargados con exito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
    finally:
        cursor.close()


def update_kpi_ingresos_acumulado_ant(conn):
    try:        
        query = "DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2023' AND Periodo = 'Acumulado' and metrica = 'ingresos';"
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        query = """
        INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT  
            'Acumulado' as Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office, 
            QRY_IND_SSS.`value`, 
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number, 
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados, 
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount, 
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,	
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta, 
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos, 
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS, 
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
            FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
            WHERE	
	        QRY_BRANCH_OFFICES.status_id = 15 AND
	        DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) < (DAY(CURDATE())) AND
	        MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate()))) AND
	        YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())-1
        GROUP BY 
	        DM_PERIODO.Periodo,
	        DM_PERIODO.period,
	        DM_PERIODO.`Año`,
	        QRY_BRANCH_OFFICES.branch_office
        ORDER BY
	        QRY_INGRESOS_TOTALES_PBI.date ASC
        """
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        st.write("Ingresos Anterior Acumulado, Cargados con exito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
    finally:
        cursor.close()


def update_kpi_ingresos_acumulado_ppto(conn):
    try:
        cursor = conn.cursor()
        # Eliminar registros
        query_delete = "DELETE FROM KPI_INGRESOS_IMG_MES WHERE metrica = 'ppto' AND periodo = 'Acumulado';"
        cursor.execute(query_delete)

        # CONFIGURAR SET_time_names
        set_locale_query = "SET lc_time_names = 'es_ES';"
        cursor.execute(set_locale_query)

        # Insertar registros
        query = """
        INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
        SELECT
        'Acumulado' AS periodo,
        CONCAT(YEAR(QRY_PPTO_DIA.date), '-', LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0')) AS period,
        YEAR(QRY_PPTO_DIA.date) AS año,
        QRY_BRANCH_OFFICES.branch_office AS branch_office,
        '0' AS `value`,
        '0' AS `ticket_number`,
        '0' AS `abonados`,
        '0' AS `net_amount`,
        '0' AS transbank,
        '0' AS Venta_Neta,
        '0' AS Ingresos,
        SUM(QRY_PPTO_DIA.ppto) AS ppto,
        '0' AS Venta_SSS,
        '0' AS Ingresos_SSS,
        'ppto' AS metrica
        FROM QRY_PPTO_DIA
        LEFT JOIN QRY_BRANCH_OFFICES
        ON QRY_PPTO_DIA.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
        WHERE
        DAY(QRY_PPTO_DIA.date) <= DAY(CURDATE()-1) AND
        MONTH(QRY_PPTO_DIA.date) = MONTH(CURDATE()) AND
        YEAR(QRY_PPTO_DIA.date) = YEAR(CURDATE())
        GROUP BY
        QRY_BRANCH_OFFICES.branch_office;
        """
        cursor.execute(query)

        # Confirmar transacción
        conn.commit()

        st.write("Ingresos Presupuesto Acumulado, Cargados con exito.")

    except Error as e:
        # Revertir transacción en caso de error
        conn.rollback()
        st.write(f"The error '{e}' occurred")

    finally:
        cursor.close()

        


#Informe de ventas - Mensuales

def update_kpi_ingresos_mes_act(conn):
    try:        
        query = "DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2024' and metrica = 'ingresos' AND periodo != 'Acumulado'"
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        query = """
        INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT
            DM_PERIODO.Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office, 
            QRY_IND_SSS.`value`, 
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number, 
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados, 
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount, 
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,	
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta, 
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos, 
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS, 
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
            FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
            WHERE	
                QRY_BRANCH_OFFICES.status_id = 15 AND
                #DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) <= (DAY(CURDATE())-1) AND
                #MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate())-1)) AND
                YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())
            GROUP BY 
                DM_PERIODO.Periodo,
                DM_PERIODO.period,
                DM_PERIODO.`Año`,
                QRY_BRANCH_OFFICES.branch_office
            ORDER BY
                QRY_INGRESOS_TOTALES_PBI.date ASC
        """
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        st.write("Ingresos Actual Mensual, Cargados con exito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
    finally:
        cursor.close()


def update_kpi_ingresos_mes_ant(conn):
    try:        
        query = "DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2023' and metrica = 'ingresos' AND periodo != 'Acumulado'"
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        query = """
        INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto , Venta_SSS, Ingresos_SSS, metrica)
            SELECT
            DM_PERIODO.Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office, 
            QRY_IND_SSS.`value`, 
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number, 
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados, 
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount, 
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,	
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta, 
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos, 
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS, 
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
            FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
            WHERE	
                QRY_BRANCH_OFFICES.status_id = 15 AND
                #DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) <= (DAY(CURDATE())-1) AND
                #MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate())-1)) AND
                YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())-1                
            GROUP BY 
                DM_PERIODO.Periodo,
                DM_PERIODO.period,
                DM_PERIODO.`Año`,
                QRY_BRANCH_OFFICES.branch_office
            ORDER BY
                QRY_INGRESOS_TOTALES_PBI.date ASC
        """
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        st.write("Ingresos Anterior Mensual, Cargados con exito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
    finally:
        cursor.close()


def update_kpi_ingresos_mes_ppto(conn):
    try:
        cursor = conn.cursor()
        # Eliminar registros
        query_delete = "DELETE FROM KPI_INGRESOS_IMG_MES WHERE metrica = 'ppto' and Period != 'Acumulado';"
        cursor.execute(query_delete)
        
        # CONFIGURAR SET_time_names
        set_locale_query = "SET lc_time_names = 'es_ES';"
        cursor.execute(set_locale_query)

        # Insertar registros
        query = """
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT
                CONCAT(LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0'),'-',UPPER(SUBSTRING(MONTHNAME(QRY_PPTO_DIA.date), 1, 1)),
                       LOWER(SUBSTRING(MONTHNAME(QRY_PPTO_DIA.date), 2))) AS periodo,
                CONCAT(YEAR(QRY_PPTO_DIA.date), '-', LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0')) AS period,
                YEAR(QRY_PPTO_DIA.date) as año,
                QRY_BRANCH_OFFICES.branch_office as branch_office,
                '0' as `value`,
                '0' as `ticket_number`,
                '0' as `abonados`,
                '0' as `net_amount`,
                '0' as transbank,
                '0' as Venta_Neta,
                '0' as Ingresos,
                SUM(QRY_PPTO_DIA.ppto) as ppto,
                '0' as Venta_SSS,
                '0' as Ingresos_SSS,
                'ppto' as metrica
            FROM QRY_PPTO_DIA
            LEFT JOIN QRY_BRANCH_OFFICES
                ON QRY_PPTO_DIA.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            WHERE
                YEAR(QRY_PPTO_DIA.date) = YEAR(curdate())
            GROUP BY
                CONCAT(YEAR(QRY_PPTO_DIA.date), '-', LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0')),
                QRY_BRANCH_OFFICES.branch_office;
        """
        cursor.execute(query)
        # Confirmar transacción
        conn.commit()        
        st.write("Ingresos Presupuesto Mensual, Cargados con exito.")
    except Error as e:
        # Revertir transacción en caso de error
        conn.rollback()
        st.write(f"The error '{e}' occurred")

    finally:
        cursor.close()


#Carga de Abonados
def update_abonados(conn):
    try:        
        cursor = conn.cursor()
        # Eliminar registros
        query_delete = "DELETE FROM TP_ABONADOS;"
        cursor.execute(query_delete)        
        # Insertar registros
        query = """INSERT INTO TP_ABONADOS
            SELECT DISTINCT
            tp_dtes.dte_id AS dte_id, 
            DATE_FORMAT(tp_dtes.created_at,"%Y-%m-%d") AS date, 
            tp_dtes.rut AS rut, 
            UPPER(users.`names`) AS cliente, 
            CONCAT_WS(" - ",tp_dtes.rut,UPPER(users.`names`)) AS razon_social, 
            tp_dtes.folio AS folio, 
            tp_dtes.branch_office_id AS branch_office_id, 
            tp_dtes.dte_type_id AS dte_type_id, 
            tp_dtes.status_id AS status_id, 
            tp_dtes.amount AS amount, 
            tp_dtes.period AS period, 
            tp_dtes.`comment` AS `comment`, 
            statuses.`status` AS `status`, 
            tp_dtes.chip_id AS chip_id
            FROM 	tp_dtes
            LEFT JOIN 	customers
            ON tp_dtes.rut = customers.rut
            LEFT JOIN users
            ON customers.rut = users.rut
            LEFT JOIN statuses
            ON tp_dtes.status_id = statuses.status_id
            WHERE
            tp_dtes.rut <> '66666666-6' AND
            tp_dtes.dte_version_id = 1 AND
            tp_dtes.status_id > 17 AND
            tp_dtes.status_id < 24 AND
            users.rol_id = 14 AND
            YEAR(tp_dtes.created_at) = (YEAR(curdate()));
        """
        
        cursor.execute(query)
        conn.commit()
        st.write("Abonados Mensuales, cargados con éxito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
        conn.rollback()
    finally:
        cursor.close()

#Carga Depositos
def update_kpi_depositos_dia(conn):
    try: 
        cursor = conn.cursor()
        # Eliminar registros
        query_delete = "DELETE FROM KPI_DTES_DEPOSITOS_DIA;"
        cursor.execute(query_delete)         
        # Insertar registros
        query = """
        INSERT INTO KPI_DTES_DEPOSITOS_DIA (Fecha, branch_office_id, deposito)
                SELECT
                    DATE_FORMAT(deposits.collection_date, '%Y-%m-%d') AS Fecha,
                    deposits.branch_office_id AS branch_office_id,
                    SUM(deposits.deposit_amount) AS deposito
                FROM deposits
                LEFT JOIN statuses ON deposits.status_id = statuses.status_id
                LEFT JOIN QRY_BRANCH_OFFICES ON deposits.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
                WHERE
                    deposits.collection_date < CURDATE() AND
                    YEAR(deposits.collection_date) = YEAR(CURDATE()) AND
                    QRY_BRANCH_OFFICES.status_id = 15
                GROUP BY
                    DATE_FORMAT(deposits.collection_date, '%Y-%m-%d'),
                    deposits.branch_office_id
        """
        
        cursor.execute(query)
        conn.commit()
        st.write("Depósitos diarios cargados con éxito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
        conn.rollback()
    finally:
        cursor.close()
        
        
def update_kpi_recaudacion_dia(conn):
    try:
        cursor = conn.cursor()
        # Eliminar registros
        query_delete = "DELETE FROM KPI_DTES_RECAUDACION_DIA;"
        cursor.execute(query_delete)           
        # Insertar registros
        query = """
        INSERT INTO KPI_DTES_RECAUDACION_DIA (Fecha, branch_office_id, recaudacion)
                SELECT
                    DATE_FORMAT(collections.created_at, '%Y-%m-%d') AS Fecha,
                    collections.branch_office_id AS branch_office_id,
                    SUM(collections.gross_amount) AS recaudacion
                FROM collections
                LEFT JOIN cashiers ON collections.cashier_id = cashiers.cashier_id
                LEFT JOIN branch_offices ON collections.branch_office_id = branch_offices.branch_office_id
                WHERE
                    collections.special_cashier = 0 AND
                    cashiers.cashier_type_id <> 3 AND
                    branch_offices.status_id = 15 AND
                    collections.created_at < CURDATE() - INTERVAL 1 DAY AND
                    YEAR(collections.created_at) = YEAR(CURDATE())
                GROUP BY
                    DATE_FORMAT(collections.created_at, '%Y-%m-%d'),
                    collections.branch_office_id
        """
        
        cursor.execute(query)
        conn.commit()
        st.write("Recaudación diaria cargada con éxito.")
    except Error as e:
        st.write(f"The error '{e}' occurred")
        conn.rollback()
    finally:
        cursor.close()

#Formulario Menu
def cargar_datos(opcion1, opcion2, opcion3):
    if opcion1 == "Informe de ventas":
        if opcion2 == "Acumulado":
            if opcion3 == "Actual":
                conn = get_db_connection()
                update_kpi_ingresos_acumulado_act(conn)
            elif opcion3 == "Año Anterior":
                conn = get_db_connection()
                update_kpi_ingresos_acumulado_ant(conn)                
            elif opcion3 == "Ppto":
                conn = get_db_connection()
                update_kpi_ingresos_acumulado_ppto(conn)
                
        elif opcion2 == "Mensual":            
            if opcion3 == "Actual":
                conn = get_db_connection()
                update_kpi_ingresos_mes_act(conn)              
            elif opcion3 == "Año Anterior":
                conn = get_db_connection() 
                update_kpi_ingresos_mes_ant(conn)               
            elif opcion3 == "Ppto":
                conn = get_db_connection()
                update_kpi_ingresos_mes_ppto(conn)
    elif opcion1 == "Abonados":
        conn = get_db_connection()
        update_abonados(conn)
    elif opcion1 == "Depositos":
        if opcion2 == "Recaudacion":
            conn = get_db_connection()
            update_kpi_recaudacion_dia(conn)
        elif opcion2 == "Depositos":
            conn = get_db_connection()
            update_kpi_depositos_dia(conn)
    else:
        st.write(f"No se encontraron datos para {opcion1} - {opcion2} - {opcion3}")
             
        
def main(authenticated=False):
    if not authenticated:
        #st.error("Necesitas autenticarte primero")
        #st.error("Necesitas autenticarte")
        #st.stop()
        raise Exception("No autenticado, Necesitas autenticarte primero")
        #return
        #st.error("Necesitas autenticarte primero")
        #return    
    else:
        st.title("Carga de datos")
        opcion1 = st.selectbox("Selecciona una opción", ["Informe de ventas", "Abonados", "Depositos"])        
        if opcion1 == "Informe de ventas":
            opcion2 = st.selectbox("Selecciona una opción", ["Acumulado", "Mensual"])
            if opcion2 == "Acumulado":
                opcion3 = st.selectbox("Selecciona una opción", ["Actual", "Año Anterior", "Ppto"])
                if st.button("Carga", key="carga_acumulado"):
                    cargar_datos(opcion1, opcion2, opcion3)
            elif opcion2 == "Mensual":
                opcion3 = st.selectbox("Selecciona una opción", ["Actual", "Año Anterior", "Ppto"])
                if st.button("Carga", key="carga_mensual"):
                    cargar_datos(opcion1, opcion2, opcion3)
        elif opcion1 == "Depositos":
            opcion2 = st.selectbox("Selecciona una opción", ["Recaudacion", "Depositos"])
            if opcion2 == "Recaudacion":
                if st.button("Carga", key="carga_recaudacion"):
                    cargar_datos(opcion1, opcion2, None)
            elif opcion2 == "Depositos":
                if st.button("Carga", key="carga_depositos"):
                    cargar_datos(opcion1, opcion2, None)
        elif opcion1 == "Abonados":
            if st.button("Carga", key="carga_abonados"):
                cargar_datos(opcion1, None, None)
                                  
    
if __name__ == "__main__":
    main()   
          
