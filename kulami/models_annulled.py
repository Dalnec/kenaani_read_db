import pyodbc
import psycopg2
from base.db import __conectarse


class Venta:
    id_venta = None
    fecha_venta = None
    codigo_tipo_proceso = None
    external_id = None
    motivo_anulacion = None

    def __str__(self):
        return "{}, {}".format(self.fecha_venta, self.codigo_tipo_proceso) #ver para que sirve

def leer_db_fanulados():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas_anulados = []

#tipodocumento.codigo_sunat,

    sql_header = """
            SELECT                 
                ventas.id_venta,
                ventas.fecha_hora,
                tipodocumento.codigo_sunat,              
                ventas.observaciones_declaracion,
                ventas.motivo_anulacion,
                ventas.codigo_cliente
            FROM
                comercial.ventas,
                comercial. tipodocumento
            WHERE
                ventas.id_tipodocumento = tipodocumento.id_tipodocumento AND
                ventas.estado_declaracion_anulado = 'PENDIENTE' AND
                ventas.codigo_cliente = 'ANULADO' AND
                tipodocumento.codigo_sunat = '01' AND
                ventas.observaciones_declaracion != ''
            ORDER BY ventas.fecha_hora
        """   
    cursor.execute(sql_header)

    for row in cursor.fetchall():
        venta = Venta()
        
        venta.id_venta = row[0]
        venta.fecha_venta = row[1]
        venta.codigo_tipo_proceso = row[2]
        venta.external_id = row[3]
        venta.motivo_anulacion = row[4]

        lista_ventas_anulados.append(venta)
    
    cursor.close()
    cnx.close()
    return _generate_lista_anulados(lista_ventas_anulados)

def leer_db_banulados():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas_anulados = []

#tipodocumento.codigo_sunat,

    sql_header = """
            SELECT                 
                ventas.id_venta,
                ventas.fecha_hora,
                tipodocumento.codigo_sunat,              
                ventas.observaciones_declaracion,
                ventas.motivo_anulacion,
                ventas.codigo_cliente
            FROM
                comercial.ventas,
                comercial. tipodocumento
            WHERE
                ventas.id_tipodocumento = tipodocumento.id_tipodocumento AND
                ventas.estado_declaracion_anulado = 'PENDIENTE' AND
                ventas.codigo_cliente = 'ANULADO' AND
                tipodocumento.codigo_sunat = '03' AND
                ventas.observaciones_declaracion != ''
            ORDER BY ventas.fecha_hora
        """   
    cursor.execute(sql_header)

    for row in cursor.fetchall():
        venta = Venta()
        
        venta.id_venta = row[0]
        venta.fecha_venta = row[1]
        venta.codigo_tipo_proceso = row[2]
        venta.external_id = row[3]
        venta.motivo_anulacion = row[4]

        lista_ventas_anulados.append(venta)
    
    cursor.close()
    cnx.close()
    return _generate_lista_anulados(lista_ventas_anulados)

def _generate_lista_anulados(ventas_anulados):
    
    header_dics = []
    for venta in ventas_anulados:
        header_dic = {}
        
        # Creamos el cuerpo del pse
        header_dic['fecha_de_emision_de_documentos'] = venta.fecha_venta.strftime('%Y-%m-%d')
        header_dic['codigo_tipo_proceso'] = int(venta.codigo_tipo_proceso)

        # documentos
        documents = {}
        documents['external_id'] = venta.external_id
        documents['motivo_anulacion'] = venta.motivo_anulacion

        header_dic['documentos'] = documents

        header_dics.append(header_dic)
    return header_dics