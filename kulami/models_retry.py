from base.db import __conectarse, get_retry_date_pgsql

def _get_retry_documentos(dia, estado):
    cnx = __conectarse()
    cursor = cnx.cursor()
    sql_header = """
            SELECT V.id_venta, V.fecha_hora, V.num_serie, V.num_documento, T.id_tipodocumento, V.estado_declaracion, V.estado_declaracion_anulado, V.external_id                 
            FROM comercial.ventas AS V, comercial.tipodocumento AS T
            WHERE T.id_tipodocumento = V.id_tipodocumento 
                --AND T.codigo_sunat = '03'
                AND (V.fecha_hora >= '{} 00:00:00') AND (V.fecha_hora <= '{} 23:59:00') --fecha obtenida
                AND (V.estado_declaracion='{}') --estado
                AND (V.estado_declaracion_anulado='PENDIENTE') --estado
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
        return date_retry, doc_list
    else:
        return None, []