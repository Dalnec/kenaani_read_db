import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

db_name = config['BASE']['DB_NAME']
db_user = config['BASE']['DB_USER']
db_pass = config['BASE']['DB_PASS']
db_host = config['BASE']['DB_HOST']
db_port = config['BASE']['DB_PORT']

def __conectarse():
    try:
        # nos conectamos a la bd del cafae
        cnx = psycopg2.connect(database=db_name, user=db_user,
                               password=db_pass, host=db_host, port=db_port)
        return cnx
    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)


def update_venta_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE comercial.ventas SET observaciones_declaracion = %s, estado_declaracion='PROCESADO' WHERE id_venta = %s", (ext_id, id))
        cnx.commit() #Guarda los cambios en la bd
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()


def read_empresa_pgsql():
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute("SELECT * FROM comercial.empresa WHERE id_empresa=%s", (1,))
        convenio = cursor.fetchone()
        return convenio
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_anulados_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE comercial.ventas SET observaciones_declaracion = %s, estado_declaracion_anulado='PROCESADO' WHERE id_venta = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_notaCredito_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE comercial.notas_credito_debito SET observaciones_declaracion = %s, estado_declaracion='PROCESADO' WHERE id_notas_credito_debito = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

