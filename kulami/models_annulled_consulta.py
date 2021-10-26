from base.db import __conectarse

class Venta:
    id_venta = None
    codigo_sunat = None
    external_id = None
    serie = None
    numero = None

    def __str__(self):
        return "{}, {}".format(self.codigo_sunat, self.external_id)

def leer_db_anulados_consultar():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas_anulados = []

    sql_header = """
            SELECT                 
                ventas.id_venta,
                tipodocumento.codigo_sunat,              
                ventas.observaciones_declaracion,
                ventas.num_serie,
                ventas.num_documento
            FROM
                comercial.ventas,
                comercial. tipodocumento
            WHERE
                ventas.id_tipodocumento = tipodocumento.id_tipodocumento AND
                ventas.estado_declaracion_anulado = 'PENDIENTE' AND
                ventas.codigo_cliente = 'ANULADO' AND
                ventas.estado_declaracion = 'ANULACION A CONSULTAR' AND
                tipodocumento.codigo_sunat in ('01', '03')
            ORDER BY ventas.fecha_hora
        """   
    cursor.execute(sql_header)

    for row in cursor.fetchall():
        venta = Venta()
        
        venta.id_venta = row[0]
        venta.codigo_sunat = row[1]
        venta.external_id = row[2]
        venta.serie = row[3]
        venta.numero = row[4]

        lista_ventas_anulados.append(venta)
    
    cursor.close()
    cnx.close()
    return _generate_lista_anulados_consultar(lista_ventas_anulados)

def _generate_lista_anulados_consultar(ventas_anulados):
    
    header_dics = []
    for venta in ventas_anulados:
        header_dic = {}
        header_dic['id_venta'] = int(venta.id_venta)
        header_dic['codigo_sunat'] = venta.codigo_sunat
        header_dic['documento'] = "{}-{}".format(venta.serie, venta.numero)
        header_dic['data'] = venta.external_id

        header_dics.append(header_dic)
    return header_dics