import pyodbc
import psycopg2
from base.db import __conectarse, read_resumen_pgsql
import time

def _ver_documentos(dia):
    cnx = __conectarse()
    cursor = cnx.cursor()
    sql_header = """
            SELECT              
                V.id_venta,
                V.fecha_hora,
                V.num_serie,
                V.num_documento,
                T.id_tipodocumento                
            FROM comercial.ventas AS V, comercial.tipodocumento AS T
            WHERE T.id_tipodocumento = V.id_tipodocumento 
                AND V.estado_declaracion='PROCESADO'
                AND V.observaciones_declaracion != '' 
                AND T.id_tipodocumento = 25 
                AND (V.fecha_hora >= '{} 00:00:00') AND (V.fecha_hora <= '{} 23:59:00')
            ORDER BY V.fecha_hora
        """
    cursor.execute(sql_header.format(dia, dia))
    estado = cursor.fetchone()
    cursor.close()
    cnx.close()
    return estado

def leer_db_resumen():    
    est_resumen = read_resumen_pgsql()
    estado_resumen = est_resumen[0].strftime('%Y-%m-%d')
    estado_resumen = time.strptime(estado_resumen, '%Y-%m-%d') 
    estado_resumen = time.mktime(estado_resumen) + 86400
    estado_resumen = time.localtime(estado_resumen) 
    estado_resumen = time.strftime("%Y-%m-%d", estado_resumen)
    antesdeayer = time.localtime(time.time() - 259200) #menos 3 dias
    antesdeayer = time.strftime("%Y-%m-%d", antesdeayer)
    last_resumen = ''
    while (estado_resumen <= antesdeayer):
        estado = _ver_documentos(estado_resumen)
        if not (estado is None):
            last_resumen = estado[1].strftime('%Y-%m-%d')
            break
        estado_resumen = time.strptime(estado_resumen, '%Y-%m-%d')
        estado_resumen = time.mktime(estado_resumen) + 86400 # Convierte a segundos  + un dia (86400)
        estado_resumen = time.localtime(estado_resumen) 
        estado_resumen = time.strftime("%Y-%m-%d", estado_resumen)
    
    return _generate_lista(last_resumen)
    
    

def _generate_lista(last_resumen):

    header_dics = []
    header_dic = {}
    #if not (last_resumen is ''):
    if (last_resumen != ''):            
        header_dic['fecha_de_emision_de_documentos'] = last_resumen
        header_dic['codigo_tipo_proceso'] = '1'
        header_dics.append(header_dic)

    return header_dics

def leer_db_consulta():
    cnx = __conectarse()
    cursor = cnx.cursor()

    #antesdeayer = time.localtime(time.time()) #- 518400)
    #antesdeayer = time.strftime("%Y-%m-%d", antesdeayer)

    #sql_header = """
    #        SELECT R.id_resumen, R.ticket, R.ext_id_resumen
    #        FROM comercial.resumen AS R
    #        WHERE fecha_hora = '{}'
    #    """
    sql_header = """SELECT id_resumen, ticket, ext_id_resumen 
                    FROM comercial.resumen WHERE filename = '' AND ticket != '' 
                    ORDER BY fecha_hora DESC LIMIT 1"""
    cursor.execute(sql_header)
    estado = cursor.fetchone()
    cursor.close()
    cnx.close()
    return _generate_consulta(estado)

def _generate_consulta(estado):

    header_dics = []
    if not (estado is None):
        header_dic = {}
        
        header_dic['id_resumen'] = estado[0]
        header_dic['ticket'] = estado[1]
        header_dic['external_id'] = estado[2]

        header_dics.append(header_dic)

    return header_dics

