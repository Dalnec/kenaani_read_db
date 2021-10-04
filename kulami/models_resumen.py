from pseapi.api import ObjJSON
import pyodbc
import psycopg2
from base.db import __conectarse, get_date_por_resumen_pgsql, get_resumen_por_consultar_pgsql
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
                T.id_tipodocumento,
                V.estado_declaracion,
                V.estado_declaracion_anulado                 
            FROM comercial.ventas AS V, comercial.tipodocumento AS T
            WHERE T.id_tipodocumento = V.id_tipodocumento 
                AND (V.estado_declaracion='POR RESUMIR' OR V.estado_declaracion='ANULADO POR RESUMIR')
                AND V.observaciones_declaracion = '' 
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


def _ver_documentos_por_consultar(json):
    cnx = __conectarse()
    cursor = cnx.cursor()
    
    sql_data = """SELECT V.id_venta,
                    V.fecha_hora,
                    V.num_serie,
                    V.num_documento,
                    V.estado_declaracion,
                    V.estado_declaracion_anulado,
                    V.observaciones_declaracion
                FROM comercial.ventas AS V
                WHERE observaciones_declaracion = '{}' """
    cursor.execute(sql_data.format(json))
    lista_consultar = cursor.fetchall()
    cursor.close()
    cnx.close()
    return lista_consultar

def leer_db_consulta():
    to_consultar = get_resumen_por_consultar_pgsql()
    if to_consultar:
        lista_consultar = _ver_documentos_por_consultar(to_consultar[1])
        return to_consultar[1], lista_consultar
    else:
        return [], []

# def _get_format(lista_consultar):
#     print(lista_consultar[4])
#     if lista_consultar[4] == 'POR CONSULTAR':
#         formato = lista_consultar[6]
#         print(formato)
#     else:
#         formato = lista_consultar[6]
#         print(formato)
#     return formato