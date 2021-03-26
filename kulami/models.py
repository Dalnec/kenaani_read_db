import pyodbc
import psycopg2
from base.db import __conectarse, read_empresa_pgsql
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
ubigeo = config['MODELS']['UBIGEO']
date_header = config['MODELS']['DATE_HEADER']

class Venta:
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
    igv = None
    igv_real = None
    total_gratuito = None
    sumSubtotales = None
    #sumSubtotalesIgv = None
    detalle_ventas = []

    def __str__(self):
        return "{} - {} {}".format(self.codigo_tipo_documento, self.serie_documento, self.detalle_ventas)


class DetalleVenta:
    def __init__(self, codigo_producto, nombre_producto, cantidad, precio_producto, unidad_medida, total_impuestos_bolsa_plastica,
                desc_individual, desc_porcentaje, sub_total, monto_total, igv, igv_descuento):
        self.codigo_producto = codigo_producto
        self.nombre_producto = nombre_producto
        self.cantidad = float(cantidad)
        self.precio_producto = float(precio_producto)
        self.unidad_medida = unidad_medida
        self.total_impuestos_bolsa_plastica = float(total_impuestos_bolsa_plastica)
        self.desc_individual = float(desc_individual)
        self.desc_porcentaje = float(desc_porcentaje)
        self.sub_total = float(sub_total)
        self.monto_total = float(monto_total)
        self.igv = float(igv)
        self.igv_descuento = float(igv_descuento)

    def __str__(self):
        return self.nombre_producto


def leer_db_access():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas = []

    sql_header = """
        SELECT  distinct                 
            V.id_venta,--0
            V.num_serie,--1
            V.num_documento,--2
            V.fecha_hora,--3
            TD.codigo_sunat,--4
            case when C.ruc !='' then '6' when C.dni !='' then '1'  else '0' end cliente_tipo_de_documento,--5           
            case when C.dni !='' then C.dni when C.ruc !='' then C.ruc else '00000000' end cliente_numero_de_documento,--6
            C.nombres_cliente,--7
            D.direccion,--8
            (case 
                when (
                    select sum(dv.monto_impuesto_bolsas) 
                    from comercial.detalle_venta dv 
                    where dv.id_venta= V.id_venta and dv.monto_impuesto_bolsas != 0) 
                is null then 0 
                else (
                    select sum(dv.monto_impuesto_bolsas)
                    from comercial.detalle_venta dv 
                    where dv.id_venta= V.id_venta and dv.monto_impuesto_bolsas != 0 ) 
                end ) + V.monto_venta total,
            V.cod_empleado,--10               
            MP.descripcion as forma_pago,--11
            V.id_puntodeventa,--12
            V.descuento,--13
            V.igv, --14
            (V.monto_venta * 0.18)::numeric(12,3) as igv_real --15
        FROM comercial.ventas V
            INNER JOIN comercial.tipodocumento TD ON TD.id_tipodocumento = V.id_tipodocumento
            INNER JOIN comercial.cliente C ON C.codigo_cliente = V.codigo_cliente
            INNER JOIN comercial.direcciones D ON D.id_direcciones = V.id_direcciones
            INNER JOIN comercial.moneda M ON M.id_moneda = V.id_moneda
            INNER JOIN comercial.detalle_venta DV ON V.id_venta = DV.id_venta
            INNER JOIN comercial.producto P ON P.codigo_producto = DV.codigo_producto
            INNER JOIN comercial.detalle_producto DP ON P.codigo_producto = DP.codigo_producto
            INNER JOIN comercial.unidadmedida U ON  U.codigo_unidad_m = DV.cod_unidad_medida 
            INNER JOIN comercial.metodo_pago MP ON  MP.id_metodo_pago = V.id_metodo_pago
        WHERE V.estado_declaracion='PENDIENTE' 
            AND V.num_serie not in ('PRE') 
            AND TD.id_tipodocumento in (25,26)
            AND V.fecha_hora >= '{}'
            AND V.codigo_cliente != 'ANULADO'
        ORDER BY V.fecha_hora 
        """
    #(1,2) (25,26)
    sql_detail = """
        SELECT distinct 
            P.codigo_producto codigo,--0
            DV.descripcion,		--1
            DV.cantidad,		--2
            DV.monto precio_unitario,--3
            case when P.impuesto_bolsas = 'TRUE' then (select parametros.valor::DECIMAL from comercial.parametros where id_parametros = 72) * DV.cantidad else 0 end impuesto_bolsas,--4
            DV. descuento_individual,--5
            porcentaje_descuento,--6
            (DV.cantidad * DV.monto) sub_total, --7
            monto_total, --8
            DV.igv, 	--9
            DV.igv_descuento --10
        FROM
            comercial.detalle_venta DV
            INNER JOIN comercial.producto P ON P.codigo_producto = DV.codigo_producto
            INNER JOIN comercial.detalle_producto DP ON P.codigo_producto = DP.codigo_producto
            INNER JOIN comercial.ventas V ON V.id_venta = DV.id_venta
        WHERE
            V.id_venta = {}
        """
    cursor.execute(sql_header.format(date_header))

    for row in cursor.fetchall():
        venta = Venta()        
        venta.id_venta = row[0]
        venta.serie_documento = row[1]
        venta.numero_documento = row[2]
        venta.fecha_venta = row[3]        
        venta.codigo_tipo_documento = row[4]
        venta.codigo_tipo_documento_identidad = row[5]
        venta.documento_cliente = row[6]
        venta.nombre_cliente = row[7]
        venta.direccion_cliente = row[8] if row[8] != None else ''
        venta.total_venta = float(row[9])
        venta.vendedor = row[10]
        venta.forma_pago = row[11]
        venta.punto_venta = row[12]
        venta.descuentos = float(row[13])
        venta.igv = float(row[14])
        venta.igv_real = float(row[15])
        venta.total_bolsa_plastica = 0.00        
        venta.total_descuentos = 0.00
        venta.total_gratuito = 0.00
        venta.sumSubtotales = 0.00
        #venta.sumSubtotalesIgv = 0.00
        detalle_ventas = []
        cursor.execute(sql_detail.format(venta.id_venta))
        for deta in cursor.fetchall():
            detalle_ventas.append(DetalleVenta(deta[0], deta[1], deta[2], deta[3], "UND", deta[4], 
                                            deta[5], deta[6], deta[7], deta[8], deta[9], deta[10]))
            venta.total_bolsa_plastica += float(deta[4]) 
            venta.total_descuentos += float(deta[5])
            venta.sumSubtotales += float(deta[2]) * float(deta[3]) #suma de precios*cantidades sin IGV
            #venta.sumSubtotalesIgv += round(float(deta[2]), 2) * round(float(deta[3]) * 1.18,2) #suma de precios*cantidades con IGV
            if deta[8] == 0:
                venta.total_gratuito += venta.sumSubtotales#float(deta[2]) * float(deta[3])
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
        if venta.total_descuentos != 0: #venta.descuentos != 0: venta.total_descuentos != 0:
            descT = []
            descuentosT = {}
            descuentosT['codigo'] = '02'
            descuentosT['descripcion'] = "Descuento Global afecta a la base imponible"
            descuentosT['factor'] = round(venta.total_descuentos/venta.sumSubtotales, 2)
            descuentosT['monto'] = round(venta.total_descuentos, 2)
            descuentosT['base'] = round(venta.sumSubtotales, 2)
            descT.append(descuentosT)
            header_dic['descuentos'] = descT
        
        # totales
        datos_totales = {}
        if venta.total_descuentos != 0: #venta.descuentos != 0:
            datos_totales['total_descuentos'] = round(venta.total_descuentos + venta.descuentos, 2)
        datos_totales['total_exportacion'] = 0.00
        datos_totales['total_operaciones_gravadas'] = 0.00 #if venta.igv == 0 else round(venta.sumSubtotales, 2)
        datos_totales['total_operaciones_inafectas'] = 0.00
        datos_totales['total_operaciones_exoneradas'] = venta.total_venta - venta.total_bolsa_plastica + round(venta.total_descuentos + venta.descuentos, 2)#if venta.igv == 0 else 0.00
        datos_totales['total_operaciones_gratuitas'] = round(venta.total_gratuito, 2)
        datos_totales['total_impuestos_bolsa_plastica'] = venta.total_bolsa_plastica
        datos_totales['total_igv'] = 0.00 #if venta.igv == 0 else round((venta.sumSubtotales - venta.total_descuentos )*0.18, 2)
        datos_totales['total_impuestos'] = 0.00 #if venta.igv == 0 else round((venta.sumSubtotales - venta.total_descuentos )*0.18, 2)
        datos_totales['total_valor'] = round(venta.sumSubtotales - venta.total_descuentos, 2) #venta.total_venta
        datos_totales['total_venta'] = venta.total_venta #round(venta.sumSubtotalesIgv,2) - round(venta.total_descuentos, 2)# venta.total_venta + venta.igv
        #TOTAL_VENTA=SumaSubtotalesSinIGV - total_descuentos + totalIGV
        if venta.igv != 0:
            # totalIGV = round((venta.sumSubtotales - venta.total_descuentos )*0.18, 2)
            datos_totales['total_operaciones_gravadas'] = round(venta.sumSubtotales, 2)
            datos_totales['total_operaciones_exoneradas'] = 0.00
            datos_totales['total_igv'] = venta.igv_real #totalIGV
            datos_totales['total_impuestos'] = venta.igv_real
            datos_totales['total_venta'] = venta.total_venta + venta.igv_real #venta.sumSubtotales - venta.total_descuentos + totalIGV
        header_dic['totales'] = datos_totales

        # detalle de venta
        lista_items = []
        for deta in venta.detalle_ventas:
            item = {}       
            item['codigo_interno'] = deta.codigo_producto
            item['descripcion'] = deta.nombre_producto
            item['codigo_producto_sunat'] = ''
            item['unidad_de_medida'] = 'NIU'
            item['cantidad'] = round(deta.cantidad, 2)
            item['codigo_tipo_precio'] = '01'
            item['valor_unitario'] = deta.precio_producto

            # descuentos por item
            if deta.desc_individual == -10:#deta.desc_individual != 0 and deta.monto_total!= 0 :
                desc = []
                descuentos = {}
                descuentos['codigo'] = '00'
                descuentos['descripcion'] = "Descuento Lineal"
                descuentos['factor'] = round(deta.desc_porcentaje / 100, 2)
                descuentos['monto'] =  round(deta.sub_total * (deta.desc_porcentaje / 100),2)
                descuentos['base'] =  round(deta.sub_total, 2) #+ deta.desc_individual #venta.sumSubtotales
                desc.append(descuentos)
                item['descuentos'] = desc

            if (deta.igv == 0 and deta.precio_producto != 0 and deta.monto_total != 0):
                lista_items.append(_detalle_items_exonerada(deta, item))
            elif (deta.igv != 0 and deta.precio_producto != 0 and deta.monto_total != 0):
                lista_items.append(_detalle_items_gravado(deta, item))
            else:
                lista_items.append(_detalle_items_gratuito(deta, item))

        header_dic['items'] = lista_items
        header_dics.append(header_dic)

    return header_dics

def _detalle_items_exonerada(deta, item):
    item["precio_unitario"] = deta.precio_producto
    item['codigo_tipo_afectacion_igv'] = '20'
    item['total_base_igv'] = deta.monto_total
    item['porcentaje_igv'] = 18
    item['total_igv'] = 0 
    item['total_impuestos_bolsa_plastica'] = deta.total_impuestos_bolsa_plastica
    item['total_impuestos'] = 0
    item['total_valor_item'] = deta.monto_total
    item['total_item'] = deta.monto_total
    return item

def _detalle_items_gravado(deta, item):
    item["precio_unitario"] = round(deta.precio_producto *1.18, 4)
    item['codigo_tipo_afectacion_igv'] = '10'
    item['total_base_igv'] = round((deta.cantidad * deta.precio_producto), 2) #round(deta.monto_total/1.18, 2)
    item['porcentaje_igv'] = 18
    item['total_igv'] = round((deta.cantidad * deta.precio_producto)*0.18, 2) #round(deta.monto_total - (deta.monto_total/1.18), 2) 
    #item['total_impuestos_bolsa_plastica'] = deta.total_impuestos_bolsa_plastica
    item['total_impuestos'] = round((deta.cantidad * deta.precio_producto)*0.18, 2)
    item['total_valor_item'] = round((deta.cantidad * deta.precio_producto), 2) #round(deta.monto_total/1.18, 2)#(deta.cantidad * deta.precio_producto)
    item['total_item'] = round((deta.cantidad * round(deta.precio_producto *1.18,2)), 2) #+ deta.desc_individual, 4)
    return item

def _detalle_items_gratuito(deta, item):
    item["valor_unitario"] = 0
    item['codigo_tipo_precio'] = '02'
    item['precio_unitario'] = 0
    item['codigo_tipo_afectacion_igv'] = '21' #'16'
    item['total_base_igv'] = deta.cantidad * deta.precio_producto if deta.igv == 0 else round(deta.cantidad * deta.precio_producto/1.18, 2)
    item['porcentaje_igv'] = 18
    item['total_igv'] = 0 if deta.igv == 0 else round(deta.monto_total - (deta.monto_total/1.18), 2) 
    item['total_impuestos_bolsa_plastica'] = deta.total_impuestos_bolsa_plastica
    item['total_impuestos'] = 0
    item['total_valor_item'] = 0
    item['total_item'] = 0
    return item