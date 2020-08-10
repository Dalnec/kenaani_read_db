import pyodbc
import psycopg2
from base.db import __conectarse, read_empresa_pgsql
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
ubigeo = config['MODELS']['UBIGEO']
date_header = config['MODELS']['DATE_HEADER']

class Venta:
    tipo_venta = None
    serie_documento = None
    numero_documento = None
    fecha_venta = None
    nombre_cliente = None
    documento_cliente = None
    direccion_cliente = None
    codigo_cliente = None
    vendedor = None
    total_venta = None
    codigo_tipo_documento = None
    id_venta = None
    codigo_tipo_documento_identidad = None
    forma_pago = None
    punto_venta = None
    descuentos = None
    total_descuentos = None
    detalle_ventas = []

    def __str__(self):
        return "{} - {} {}".format(self.tipo_venta, self.serie_documento, self.detalle_ventas)


class DetalleVenta:
    def __init__(self, codigo_producto, nombre_producto, cantidad, precio_producto, unidad_medida, 
        sub_total, total_impuestos_bolsa_plastica, desc_individual, desc_porcentaje, monto_total):
        self.codigo_producto = codigo_producto
        self.nombre_producto = nombre_producto
        self.cantidad = float(cantidad)
        self.precio_producto = float(precio_producto)
        self.unidad_medida = unidad_medida
        self.total_impuestos_bolsa_plastica = total_impuestos_bolsa_plastica
        self.desc_individual = desc_individual
        self.desc_porcentaje = desc_porcentaje
        self.sub_total = sub_total
        self.monto_total = monto_total

    def __str__(self):
        return self.nombre_producto


def leer_db_access():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas = []

    sql_header = """
            SELECT  distinct                 
                ventas.id_venta,
                ventas.fecha_hora,
                'generar_comprobante' AS operacion,
                tipodocumento.codigo_sunat,
                ventas.num_serie,
                ventas.num_documento,
                case when cliente.dni !='' then  cliente.dni when cliente.ruc !='' then cliente.ruc else '00000000' end cliente_numero_de_documento,
                cliente.nombres_cliente,
                direcciones.direccion,
                cliente.codigo_cliente,
                ventas.cod_empleado,
                ( case when (select sum(dv.monto_impuesto_bolsas) from comercial.detalle_venta dv where dv.id_venta= ventas.id_venta and dv.monto_impuesto_bolsas != 0) is null then 0 else (select sum(dv.monto_impuesto_bolsas)from comercial.detalle_venta dv where dv.id_venta= ventas.id_venta and dv.monto_impuesto_bolsas != 0 ) end ) + ventas.monto_venta total,
                case when cliente.ruc !='' then '6' when cliente.dni !='' then '1'  else '0' end cliente_tipo_de_documento,
                metodo_pago.descripcion as forma_pago,
                ventas.id_puntodeventa,
                ventas.descuento
                
            FROM comercial.ventas
            INNER JOIN comercial.tipodocumento ON  tipodocumento.id_tipodocumento = ventas.id_tipodocumento
            INNER JOIN comercial.cliente ON cliente.codigo_cliente = ventas.codigo_cliente
            INNER JOIN comercial.direcciones ON direcciones.id_direcciones = ventas.id_direcciones
            INNER JOIN comercial.moneda ON moneda.id_moneda = ventas.id_moneda
            INNER JOIN comercial.detalle_venta ON ventas.id_venta = detalle_venta.id_venta
            INNER JOIN comercial.producto ON producto.codigo_producto = detalle_venta.codigo_producto
            INNER JOIN comercial.detalle_producto ON producto.codigo_producto = detalle_producto.codigo_producto
            INNER JOIN comercial.unidadmedida ON  unidadmedida.codigo_unidad_m = detalle_venta.cod_unidad_medida 
            INNER JOIN comercial.metodo_pago ON  metodo_pago.id_metodo_pago = ventas.id_metodo_pago
            WHERE ventas.estado_declaracion='PENDIENTE' AND ventas.num_serie not in ('PRE') AND tipodocumento.id_tipodocumento in (25,26)
            AND ventas.fecha_hora >= '{}'
            AND ventas.codigo_cliente != 'ANULADO'
            ORDER BY ventas.fecha_hora 
        """
    #(1,2) (25,26)
    sql_detail = """
            SELECT distinct 
                producto.codigo_producto codigo,
                detalle_venta.descripcion,
                detalle_venta.cantidad,
                detalle_venta.monto precio_unitario,
                (detalle_venta.monto * detalle_venta.cantidad - detalle_venta.descuento_total) sub_total,
                case when producto.impuesto_bolsas = 'TRUE' then (select parametros.valor::DECIMAL from comercial.parametros where id_parametros = 72) * detalle_venta.cantidad else 0 end impuesto_bolsas,
                (detalle_venta.monto * detalle_venta.cantidad - detalle_venta.descuento_total ) total,
                detalle_venta. descuento_individual,
                porcentaje_descuento,
                monto_total
            FROM
                comercial.detalle_venta,
                comercial.producto,
                comercial.detalle_producto,
                comercial.ventas
            WHERE
                ventas.id_venta = detalle_venta.id_venta AND
                producto.codigo_producto = detalle_venta.codigo_producto AND
                producto.codigo_producto = detalle_producto.codigo_producto AND ventas.id_venta = {}
        """
    cursor.execute(sql_header.format(date_header))

    for row in cursor.fetchall():
        venta = Venta()
        venta.tipo_venta = row[3]
        venta.id_venta = row[0]
        venta.serie_documento = row[4]
        venta.numero_documento = row[5]
        venta.codigo_tipo_documento = row[3]
        venta.fecha_venta = row[1]
        venta.nombre_cliente = row[7]
        venta.documento_cliente = row[6]
        venta.direccion_cliente = row[8] if row[8] != None else ''
        venta.codigo_cliente = row[9]
        venta.total_venta = row[11]
        venta.total_bolsa_plastica = 0
        venta.vendedor = row[10]
        venta.codigo_tipo_documento_identidad = row[12]
        venta.forma_pago = row[13]
        venta.punto_venta = row[14]
        venta.descuentos = row[15]        
        venta.total_descuentos = 0 
        detalle_ventas = []
        cursor.execute(sql_detail.format(venta.id_venta))
        for deta in cursor.fetchall():
            detalle_ventas.append(DetalleVenta(deta[0],
                                            deta[1], deta[2], deta[3], "UND", deta[4], 
                                            deta[5], deta[7], deta[8], deta[9]))
            venta.total_bolsa_plastica += deta[5] 
            venta.total_descuentos += deta[7]
        venta.detalle_ventas = detalle_ventas
        lista_ventas.append(venta)
    
    cursor.close()
    cnx.close()
    return _generate_lista(lista_ventas)

def _generate_lista(ventas):
    
    header_dics = []
    for venta in ventas:
        codigo_tipo_operacion = '0101'
        codigo_tipo_moneda = 'PEN'
        header_dic = {}

        # Opcionales
        header_dic['id_venta'] = int(venta.id_venta)
        header_dic['informacion_adicional'] = "Forma de pago:"+ venta.forma_pago +"|Caja: "+ venta.punto_venta
        # Creamos el cuerpo del pse
        header_dic['serie_documento'] = venta.serie_documento
        header_dic['numero_documento'] = int(venta.numero_documento)
        header_dic['fecha_de_emision'] = venta.fecha_venta.strftime('%Y-%m-%d')
        header_dic['hora_de_emision'] = venta.fecha_venta.strftime('%H:%M:%S')
        header_dic['codigo_tipo_operacion'] = codigo_tipo_operacion
        header_dic['codigo_tipo_documento'] = venta.codigo_tipo_documento
        header_dic['codigo_tipo_moneda'] = codigo_tipo_moneda
        header_dic['fecha_de_vencimiento'] = venta.fecha_venta.strftime('%Y-%m-%d')
        header_dic['numero_orden_de_compra'] = ''

        # datos del cliente
        datos_del_cliente = {}
        datos_del_cliente['codigo_tipo_documento_identidad'] = venta.codigo_tipo_documento_identidad
        datos_del_cliente['numero_documento'] = venta.documento_cliente
        datos_del_cliente['apellidos_y_nombres_o_razon_social'] = venta.nombre_cliente
        datos_del_cliente['codigo_pais'] = 'PE'
        datos_del_cliente['ubigeo'] = ubigeo
        datos_del_cliente['direccion'] = venta.direccion_cliente
        datos_del_cliente['correo_electronico'] = ''
        datos_del_cliente['telefono'] = ''

        header_dic['datos_del_cliente_o_receptor'] = datos_del_cliente
        
        # descuentos Total
        if venta.descuentos != 0:
            descT = []
            descuentosT = {}
            descuentosT['codigo'] = '03'
            descuentosT['descripcion'] = "Descuento Global no afecta a la base imponible"
            descuentosT['factor'] = float(venta.descuentos / venta.total_venta)
            descuentosT['monto'] = float(venta.descuentos)
            descuentosT['base'] = float(venta.total_venta)
            descT.append(descuentosT)
            header_dic['descuentos'] = descT
        
        # totales
        datos_totales = {}
        if venta.descuentos != 0: 
            datos_totales['total_descuentos'] = round((float(venta.total_descuentos) + float(venta.descuentos)), 2)
        datos_totales['total_exportacion'] = 0.00
        datos_totales['total_operaciones_gravadas'] = 0.00
        datos_totales['total_operaciones_inafectas'] = 0.00
        datos_totales['total_operaciones_exoneradas'] = venta.total_venta - venta.total_bolsa_plastica
        datos_totales['total_operaciones_gratuitas'] = 0.00
        datos_totales['total_impuestos_bolsa_plastica'] = venta.total_bolsa_plastica
        datos_totales['total_igv'] = 0.00
        datos_totales['total_impuestos'] = 0.00
        datos_totales['total_valor'] = venta.total_venta
        datos_totales['total_venta'] = venta.total_venta

        header_dic['totales'] = datos_totales

        lista_items = []
        for deta in venta.detalle_ventas:
            item = {}       
            item['codigo_interno'] = deta.codigo_producto
            item['descripcion'] = deta.nombre_producto
            item['codigo_producto_sunat'] = ''
            item['codigo_producto_gsl'] = ''
            item['unidad_de_medida'] = 'NIU'
            item['cantidad'] = round((deta.cantidad), 2)
            item["valor_unitario"] = deta.precio_producto
            item['codigo_tipo_precio'] = '01'
            item['precio_unitario'] = deta.precio_producto
            item['codigo_tipo_afectacion_igv'] = '20'

            # descuentos por item
            if venta.total_descuentos != 0:
                desc = []
                descuentos = {}
                descuentos['codigo'] = '00'
                descuentos['descripcion'] = "Descuento Lineal"
                descuentos['factor'] = float(deta.desc_porcentaje / 100)
                descuentos['monto'] = float(deta.desc_individual)
                descuentos['base'] = float(deta.sub_total)
                desc.append(descuentos)
                item['descuentos'] = desc
                
            item['total_base_igv'] = float(deta.sub_total) - float(deta.desc_individual)
            item['porcentaje_igv'] = 18
            item['total_igv'] = 0
            item['total_impuestos_bolsa_plastica'] = deta.total_impuestos_bolsa_plastica
            item['total_impuestos'] = 0
            item['total_valor_item'] = (deta.cantidad * deta.precio_producto)
            item['total_item'] = round(float(deta.monto_total), 2)
            lista_items.append(item)

        header_dic['items'] = lista_items
        header_dics.append(header_dic)

    return header_dics
