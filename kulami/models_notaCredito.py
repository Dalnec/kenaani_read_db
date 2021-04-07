import pyodbc
import psycopg2
from base.db import __conectarse, read_empresa_pgsql


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
    codigo_tipo_documento = None
    id_notas_credito_debito = None
    codigo_tipo_documento_identidad = None
    forma_pago = None
    punto_venta = None
    motivo_o_sustento_de_nota = None
    external_id = None
    codigo_tipo_nota = None
    sumIGVs = None
    sumSubtotales = None
    sumSubtotales_igv = None
    serie_afectado = None
    numero_afectado = None
    sumDescuentos = None
    detalle_ventas = []

    def __str__(self):
        return "{} - {} {}".format(self.tipo_venta, self.serie_documento, self.detalle_ventas)

class DetalleVenta:
    def __init__(self, codigo_producto, nombre_producto, cantidad, valor_unitario, 
                precio_unitario, descuento_con_igv, sub_total_sin_igv, igv, sub_total_con_igv, 
                cod_afectacion, cod_sunat, descuento_sin_igv):
        self.codigo_producto = codigo_producto #0
        self.nombre_producto = nombre_producto #1
        self.cantidad = float(cantidad) #2
        self.valor_unitario = float(valor_unitario) #3
        self.precio_unitario = float(precio_unitario) #4
        self.descuento_con_igv = float(descuento_con_igv) #5
        self.sub_total_sin_igv = float(sub_total_sin_igv) #6
        self.igv = float(igv) #7
        self.sub_total_con_igv = float(sub_total_con_igv) #8
        self.cod_afectacion = cod_afectacion #9
        self.cod_sunat = cod_sunat #10
        self.descuento_sin_igv = float(descuento_sin_igv) #11

    def __str__(self):
        return self.nombre_producto


def leer_db_notaCredito():
    cnx = __conectarse()
    cursor = cnx.cursor()
    lista_ventas = []

    sql_header = """
            SELECT 
                N.id_notas_credito_debito, --0
                N.serie, --1
                N.numero, --2
                N.fecha, --3
                N.codigo_motivo, --4
                N.motivo, --5
                V.observaciones_declaracion, --6
                N.ruc, --7
                N.persona, --8 
                N.direccion, --9
                N.id_puntodeventa, --10
                V.num_serie, --11
                V.num_documento --12
            FROM comercial.notas_credito_debito AS N,
                comercial.ventas as V
            WHERE N.id_referencia = V.id_venta
                AND N.estado_declaracion = 'PENDIENTE'
                AND N.observaciones_declaracion = ''
            ORDER BY N.id_notas_credito_debito
        """

    sql_detail = """
            SELECT             
                dnc.codigo_producto as itco_codigo_interno, --0          
                dnc.descripcion as itco_descripcion,  --1
                dnc.cantidad::numeric(12,3) as itco_cantidad,  --2
                (case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                    case when nc.igv_incluido_precio = 'S' then           
                        (dnc.precio_unitario / ( (taza_igv / 100) + 1 ))::numeric(10,3)           
                    else           
                        dnc.precio_unitario          
                    end           
                else dnc.precio_unitario end )::numeric(12,3) as itco_valor_unitario, --3          
                    
                (case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                case when nc.igv_incluido_precio = 'S' then           
                    dnc.precio_unitario          
                else           
                    (dnc.precio_unitario * ( (taza_igv / 100) + 1 ))::numeric(10,3)           
                end           
                else dnc.precio_unitario end )::numeric(12,3) as itco_precio_unitario, --4   
                (dnc.descuento)::numeric(12,3) as itco_descuento, --5     
                ((dnc.cantidad * dnc.precio_unitario) - (dnc.descuento) -       
                ( case when dnc.igv > 0 AND nc.con_igv = 'S' then        
                case when nc.igv_incluido_precio = 'S' then        
                    ((dnc.cantidad * dnc.precio_unitario) - (dnc.descuento))        
                    -        
                    (((dnc.cantidad * dnc.precio_unitario) - (dnc.descuento)) / ( (taza_igv / 100) + 1 ))       
                else        
                    0       
                end        
                else 0 end))::numeric(12,3) as  itco_sub_total, --6         
                
                (case when emp.exonerado_igv = 'N' then           
                    case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                        dnc.igv          
                    else           
                        0          
                    end           
                else          
                    case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                        dnc.igv          
                    else           
                        0         
                    end          
                end) as itco_igv, --7          
                        
                (((dnc.cantidad * dnc.precio_unitario) - (dnc.descuento) -       
                ( case when dnc.igv > 0 AND nc.con_igv = 'S' then        
                case when nc.igv_incluido_precio = 'S' then        
                    ((dnc.cantidad * dnc.precio_unitario) - (dnc.descuento))        
                    -        
                    (((dnc.cantidad * dnc.precio_unitario) - (dnc.descuento)) / ( (taza_igv / 100) + 1 ))       
                else        
                    0       
                end        
                else 0 end))::numeric(12,3) +      
                
                (case when emp.exonerado_igv = 'N' then           
                    case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                        dnc.igv          
                    else           
                        0          
                    end           
                else          
                    case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                        dnc.igv          
                    else           
                        0         
                    end          
                end))::numeric(12,3) as itco_total, --8  
                (case    
                    when nc.con_igv = 'S' AND pp.cobrar_igv = 'A' then          
                        '10'         
                    when nc.con_igv = 'S' AND pp.cobrar_igv = 'E' then         
                        '20'        
                    when nc.con_igv = 'S' AND pp.cobrar_igv = 'I' then         
                        '30'        
                    when nc.con_igv = 'N' then         
                        '20'        
                end) as tiai_codigo, --9    
                pp.codigo_sunat as itco_codigo_sunat, --10 
                (case when nc.con_igv = 'S' AND dnc.igv > 0 then           
                    case when nc.igv_incluido_precio = 'S' then           
                        ((dnc.descuento)::numeric(12,3) / ( (taza_igv / 100) + 1 ))::numeric(10,3)           
                    else           
                        (dnc.descuento)::numeric(12,3)          
                    end           
                else dnc.precio_unitario end )::numeric(12,3) as itco_descuento_sin_igv --11     
                    
            FROM comercial.view_nota nc     
            INNER JOIN comercial.ventas vv on vv.id_venta = nc.id_referencia     
            INNER JOIN comercial.detalle_notas_credito_debito dnc on nc.id_notas_credito_debito = dnc.id_notas_credito_debito         
            INNER JOIN comercial.empleado ee on vv.cod_empleado = ee.cod_empleado         
            INNER JOIN comercial.cliente c on vv.codigo_cliente_anulado = c.codigo_cliente         
            INNER JOIN comercial.tipodocumento td on nc.id_tipodocumento = td.id_tipodocumento      
            INNER JOIN comercial.metodo_pago mp on vv.id_metodo_pago = mp.id_metodo_pago       
            INNER JOIN comercial.empresa emp on vv.id_empresa = emp.id_empresa       
            LEFT JOIN comercial.producto pp on pp.codigo_producto = dnc.codigo_producto         
            WHERE td.comprobante_electronico = 'S' 
                AND td.es_contable = 'S' 
                AND nc.clase = 'EMITIR' 
                AND  nc.id_notas_credito_debito = {}
        """
    cursor.execute(sql_header)
    for row in cursor.fetchall():
        venta = Venta()

        venta.id_notas_credito_debito = row[0]
        venta.serie_documento = row[1]
        venta.numero_documento = row[2]
        venta.fecha_venta = row[3]
        venta.codigo_tipo_nota = row[4]
        venta.motivo_o_sustento_de_nota = row[5]
        venta.external_id = row[6]    
        venta.documento_cliente = row[7]
        venta.nombre_cliente = row[8]
        venta.direccion_cliente = row[9] if row[9] != None else ''
        venta.sumIGVs = 0.0
        venta.sumSubtotales = 0.0
        venta.sumSubtotales_igv = 0.0
        venta.sumDescuentos = 0.0
        venta.forma_pago = ''
        venta.punto_venta = row[10]

        venta.serie_afectado = row[11]
        venta.numero_afectado = row[12]

        detalle_ventas = []
        cursor.execute(sql_detail.format(venta.id_notas_credito_debito))
        for deta in cursor.fetchall():
            detalle_ventas.append(
                DetalleVenta(deta[0], deta[1], deta[2], deta[3],
                            deta[4], deta[5], deta[6], deta[7],
                            deta[8], deta[9], deta[10], deta[11])
            )            
            venta.sumSubtotales += float(deta[6])
            venta.sumIGVs += float(deta[7]) 
            venta.sumSubtotales_igv += float(deta[8])
            venta.sumDescuentos += float(deta[11])
        venta.detalle_ventas = detalle_ventas
        lista_ventas.append(venta)
    
    cursor.close()
    cnx.close()
    return _generate_lista(lista_ventas)

def _generate_lista(ventas):
    
    header_dics = []
    for venta in ventas:
        codigo_tipo_documento = '07' 
        codigo_tipo_moneda = 'PEN'
        header_dic = {}

        # Opcionales
        header_dic['id_venta'] = int(venta.id_notas_credito_debito)
        header_dic['informacion_adicional'] = "Forma de pago:"+ venta.forma_pago +"|Caja: "+ venta.punto_venta

        # Creamos el cuerpo del pse
        header_dic['serie_documento'] = venta.serie_documento
        header_dic['numero_documento'] = int(venta.numero_documento)
        header_dic['fecha_de_emision'] = venta.fecha_venta.strftime('%Y-%m-%d')
        header_dic['hora_de_emision'] = venta.fecha_venta.strftime('%H:%M:%S')
        header_dic['codigo_tipo_documento'] = codigo_tipo_documento
        header_dic['codigo_tipo_nota'] = venta.codigo_tipo_nota
        header_dic['motivo_o_sustento_de_nota'] = venta.motivo_o_sustento_de_nota
        header_dic['codigo_tipo_moneda'] = codigo_tipo_moneda
        header_dic['numero_orden_de_compra'] = ''
        
        # external_id
        documento_afectado = {}
        #documento_afectado['external_id'] = venta.external_id
        documento_afectado['serie_documento'] = venta.serie_afectado
        documento_afectado['numero_documento'] = venta.numero_afectado
        documento_afectado['codigo_tipo_documento'] = '01'
        header_dic['documento_afectado'] = documento_afectado

        # descuentos Total
        if venta.sumDescuentos != 0:
            descT = []
            descuentosT = {}
            descuentosT['codigo'] = '02'
            descuentosT['descripcion'] = "Descuento Global afecta a la base imponible"
            descuentosT['factor'] = round(venta.sumDescuentos/venta.sumSubtotales, 2)
            descuentosT['monto'] = round(venta.sumDescuentos, 2)
            descuentosT['base'] = round(venta.sumSubtotales, 2)
            descT.append(descuentosT)
            header_dic['descuentos'] = descT
        
        # totales
        datos_totales = {}
        if venta.sumDescuentos != 0:
            datos_totales['total_descuentos'] = round(venta.sumDescuentos, 2)
        datos_totales['total_exportacion'] = 0
        datos_totales['total_operaciones_gravadas'] = 0
        datos_totales['total_operaciones_inafectas'] = 0
        datos_totales['total_operaciones_exoneradas'] = venta.sumSubtotales
        datos_totales['total_operaciones_gratuitas'] = 0
        datos_totales['total_igv'] = 0
        datos_totales['total_impuestos'] = 0
        datos_totales['total_valor'] = venta.sumSubtotales
        datos_totales['total_venta'] = venta.sumSubtotales

        if venta.sumIGVs != 0:
            datos_totales['total_operaciones_gravadas'] = venta.sumSubtotales
            datos_totales['total_operaciones_exoneradas'] = 0.00
            datos_totales['total_igv'] = venta.sumIGVs
            datos_totales['total_impuestos'] = venta.sumIGVs
            datos_totales['total_venta'] = venta.sumSubtotales_igv
        
        header_dic['totales'] = datos_totales

        # datos del cliente
        datos_del_cliente = {}
        datos_del_cliente['codigo_tipo_documento_identidad'] = '6'
        datos_del_cliente['numero_documento'] = venta.documento_cliente
        datos_del_cliente['apellidos_y_nombres_o_razon_social'] = venta.nombre_cliente
        datos_del_cliente['codigo_pais'] = 'PE'
        datos_del_cliente['ubigeo'] = ''
        datos_del_cliente['direccion'] = venta.direccion_cliente
        datos_del_cliente['correo_electronico'] = ''
        datos_del_cliente['telefono'] = ''

        header_dic['datos_del_cliente_o_receptor'] = datos_del_cliente
        
        lista_items = []
        for deta in venta.detalle_ventas:
            item = {}       
            item['codigo_interno'] = deta.codigo_producto
            item['descripcion'] = deta.nombre_producto
            item['codigo_producto_sunat'] = ''
            item['unidad_de_medida'] = 'NIU'
            item['cantidad'] = round(deta.cantidad, 2)
            item['codigo_tipo_precio'] = '01'
            item['valor_unitario'] = deta.valor_unitario            
            item["precio_unitario"] = deta.precio_unitario

            if (deta.igv == 0 and deta.cod_afectacion == '20'):
                lista_items.append(_detalle_items_exonerada(deta, item))
            elif (deta.igv != 0 and deta.cod_afectacion == '10'):
                lista_items.append(_detalle_items_gravado(deta, item))

        header_dic['items'] = lista_items
        header_dics.append(header_dic)

    return header_dics

def _detalle_items_exonerada(deta, item):
    item['codigo_tipo_afectacion_igv'] = '20'
    item['total_base_igv'] = deta.sub_total_sin_igv
    item['porcentaje_igv'] = 18
    item['total_igv'] = 0 
    item['total_impuestos'] = 0
    item['total_valor_item'] = deta.sub_total_sin_igv
    item['total_item'] = deta.sub_total_sin_igv
    return item

def _detalle_items_gravado(deta, item):
    item['codigo_tipo_afectacion_igv'] = '10'
    item['total_base_igv'] = deta.sub_total_sin_igv
    item['porcentaje_igv'] = 18
    item['total_igv'] = deta.igv
    item['total_impuestos'] = deta.igv
    item['total_valor_item'] = deta.sub_total_sin_igv
    item['total_item'] = deta.sub_total_con_igv
    return item