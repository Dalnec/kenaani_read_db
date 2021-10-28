from base.db import __conectarse, get_date_por_resumen_pgsql, get_resumen_por_consultar_pgsql, get_retry_date_pgsql

def _get_retry_documentos(dia, estado):
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
                AND V.observaciones_declaracion = ''
                AND T.codigo_sunat = '03'
                AND (V.fecha_hora >= '{} 00:00:00') AND (V.fecha_hora <= '{} 23:59:00') --fecha obtenida
                AND (V.estado_declaracion='{}') --estado
            ORDER BY V.fecha_hora;
        """
    cursor.execute(sql_header.format(dia, dia, estado))
    lista = cursor.fetchall()
    cursor.close()
    cnx.close()
    return lista

def leer_db_to_retry():
    # obtiene la fecha de una boleta por retry   
    retry_data = get_retry_date_pgsql()
    if retry_data:
        # damos formato a fecha obtenida y obtenemos la lista
        # de la fecha obtenida.
        date_retry = retry_data[0].strftime('%Y-%m-%d')        
        doc_list = _get_retry_documentos(date_retry, retry_data[1])
        # retornamos formato json y lista de boletas
        return date_retry, doc_list
    else:
        return None, []

# def _generate_formato(date_resumen):
#     header_dic = {}         
#     header_dic['fecha_de_emision_de_documentos'] = date_resumen
#     header_dic['codigo_tipo_proceso'] = '1'
#     return header_dic


# def _ver_documentos_por_consultar(json):
#     cnx = __conectarse()
#     cursor = cnx.cursor()
    
#     sql_data = """SELECT V.id_venta,
#                     V.fecha_hora,
#                     V.num_serie,
#                     V.num_documento,
#                     V.estado_declaracion,
#                     V.estado_declaracion_anulado,
#                     V.observaciones_declaracion
#                 FROM comercial.ventas AS V
#                 WHERE observaciones_declaracion = '{}' """
#     cursor.execute(sql_data.format(json))
#     lista_consultar = cursor.fetchall()
#     cursor.close()
#     cnx.close()
#     return lista_consultar

# def leer_db_consulta():
#     to_consultar = get_resumen_por_consultar_pgsql()
#     if to_consultar and to_consultar[1] != '':
#         lista_consultar = _ver_documentos_por_consultar(to_consultar[1])
#         return to_consultar[1], lista_consultar
#     else:
#         return [], []