import psycopg2
import configparser
import time

config = configparser.ConfigParser()
config.read('config.ini')

db_name = config['BASE']['DB_NAME']
db_user = config['BASE']['DB_USER']
db_pass = config['BASE']['DB_PASS']
db_host = config['BASE']['DB_HOST']
db_port = config['BASE']['DB_PORT']

def _get_time(format):
        timenow = time.localtime()
        if format == 1:
            timenow = time.strftime("%Y/%m/%d %H:%M:%S", timenow)
        else:
            timenow = time.strftime("%H:%M:%S", timenow)
        return timenow

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
        cursor.execute("SELECT efactur_empresa, efactur_url FROM comercial.empresa WHERE id_empresa=%s", (1,))
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

def update_rechazados_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE comercial.ventas SET observaciones_declaracion = %s WHERE id_venta = %s", (ext_id, id))
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

def insert_resumen_pgsql(tipo, ticket, ext_id, fecha): 
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "INSERT INTO comercial.resumen (tipo, ticket, ext_id_resumen, fecha_hora) VALUES (%s, %s, %s, %s)", (tipo, ticket, ext_id, fecha))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_consulta_pgsql(id, filename, ext_id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE comercial.resumen SET filename = %s, ext_id_consulta = %s WHERE id_resumen = %s", (filename, ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def read_resumen_pgsql():
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute("SELECT fecha_hora FROM comercial.resumen WHERE tipo = 'B' ORDER BY fecha_hora DESC LIMIT 1")
        convenio = cursor.fetchone()
        return convenio
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_guia_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute(
            "UPDATE comercial.guia SET razonsocial = %s WHERE id_guia = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()