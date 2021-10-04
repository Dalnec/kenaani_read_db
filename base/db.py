import psycopg2
import configparser
import time
from logger import log

config = configparser.ConfigParser()
config.read('config.ini')

db_name = config['BASE']['DB_NAME']
db_user = config['BASE']['DB_USER']
db_pass = config['BASE']['DB_PASS']
db_host = config['BASE']['DB_HOST']
db_port = config['BASE']['DB_PORT']

def _get_time():
        timenow = time.localtime()
        timenow = time.strftime("%H:%M:%S", timenow)
        return timenow

def __conectarse():
    try:
        cnx = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
        return cnx
    except (Exception, psycopg2.Error) as error:
        log.debug(f'Connection exception {error}')


def update_venta_pgsql(estado, ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute( "UPDATE comercial.ventas SET estado_declaracion = %s, external_id = %s WHERE id_venta = %s", (estado, ext_id, id))
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

def update_anulados_pgsql(estado, estado_anulado, ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute( "UPDATE comercial.ventas SET estado_declaracion = %s, estado_declaracion_anulado=%s, observaciones_declaracion = %s WHERE id_venta = %s", (estado, estado_anulado, ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_rechazados_pgsql(estado, ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute( "UPDATE comercial.ventas SET estado_declaracion = %s, estado_declaracion_anulado=%s WHERE id_venta = %s", (estado, ext_id, id))
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
        cursor.execute( "UPDATE comercial.notas_credito_debito SET observaciones_declaracion = %s, estado_declaracion='PROCESADO' WHERE id_notas_credito_debito = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_guia_pgsql(ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute( "UPDATE comercial.guia SET razonsocial = %s WHERE id_guia = %s", (ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def get_date_por_resumen_pgsql():
    try:
        datenow = time.localtime()
        datenow = time.strftime("%Y-%m-%d", datenow)
        cnx = __conectarse()
        cursor = cnx.cursor()
        consulta = """ SELECT fecha_hora FROM comercial.ventas WHERE estado_declaracion in ('POR RESUMIR' , 'ANULADO POR RESUMIR') AND fecha_hora < '{}' ORDER BY fecha_hora LIMIT 1 """
        cursor.execute(consulta.format(datenow))
        date_resumen = cursor.fetchone()
        return date_resumen
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_resumen_pgsql(estado, ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute( "UPDATE comercial.ventas SET estado_declaracion = %s, observaciones_declaracion = %s WHERE id_venta = %s", (estado, ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def get_resumen_por_consultar_pgsql():
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute("SELECT id_venta, observaciones_declaracion FROM comercial.ventas WHERE estado_declaracion = 'POR CONSULTAR' OR estado_declaracion = 'ANULADO POR RESUMIR' ORDER BY fecha_hora LIMIT 1")
        return cursor.fetchone()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()

def update_consultar_pgsql(estado, ext_id, id):
    try:
        cnx = __conectarse()
        cursor = cnx.cursor()
        cursor.execute( "UPDATE comercial.ventas SET estado_declaracion = %s, observaciones_declaracion = %s WHERE id_venta = %s", (estado, ext_id, id))
        cnx.commit()
    finally:
        # closing database connection
        if (cnx):
            cursor.close()
            cnx.close()