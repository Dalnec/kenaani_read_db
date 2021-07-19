import pyodbc
import psycopg2
from base.db import __conectarse, get_date_por_resumen_pgsql
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
                AND V.estado_declaracion='POR RESUMIR'
                AND V.observaciones_declaracion != '' 
                AND T.codigo_sunat = '03'
                AND (V.fecha_hora >= '{} 00:00:00') AND (V.fecha_hora <= '{} 23:59:00') --fecha obtenida
            ORDER BY V.fecha_hora;
        """
    cursor.execute(sql_header.format(dia, dia))
    lista = cursor.fetchall()
    cursor.close()
    cnx.close()
    return lista

def leer_db_resumen():
    # obtiene la fecha de una boleta por resumir    
    date_resumen = get_date_por_resumen_pgsql()
    if date_resumen:
        # damos formato a fecha obtenida y obtenemos la lista
        # de la fecha obtenida.
        date_resumen = date_resumen[0].strftime('%Y-%m-%d')        
        lista_boletas = _ver_documentos(date_resumen)
        # retornamos formato json y lista de boletas
        return _generate_formato(date_resumen), lista_boletas
    else:
        return [], []

def _generate_formato(date_resumen):
    header_dic = {}         
    header_dic['fecha_de_emision_de_documentos'] = date_resumen
    header_dic['codigo_tipo_proceso'] = '1'
    return header_dic

def leer_db_consulta():
    cnx = __conectarse()
    cursor = cnx.cursor()
    
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

