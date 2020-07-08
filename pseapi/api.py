import requests

from base.db import (
    read_empresa_pgsql,
    update_venta_pgsql,
    update_anulados_pgsql,
    update_notaCredito_pgsql,
    insert_resumen_pgsql,
    update_consulta_pgsql
)
from urllib3.exceptions import InsecureRequestWarning

# Disable flag warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def create_document(header_dics):
    "crea boletas y facturas"
    convenio = read_empresa_pgsql()
    url = convenio[1] + "/api/documents"
    token = convenio[0]
    _send_cpe(url, token, header_dics)


def _send_cpe(url, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    #cont = 0
    for venta in data:
        #if cont == 0:
            print(venta)
            response = requests.post(
                url, json=venta, headers=header, verify=False)
            if response.status_code == 200:
                r_json=response.json()
                external_id=r_json['data']['external_id']
                update_venta_pgsql(external_id, int(venta['id_venta']))
                print(response.content)
            else:
                print(response.status_code)
        #cont += 1


def create_anulados(header_dics, tipo):
    "crea facturas y boletas anuladas"
    convenio = read_empresa_pgsql()
    if tipo == 1 :
        url = convenio[1] + "/api/voided"
    else:
        url = convenio[1] + "/api/summaries"
    token = convenio[0]
    _send_cpe_anulados(url, token, header_dics)


def _send_cpe_anulados(url, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    #cont = 0
    for venta in data:
        #if cont == 0:
            response = requests.post(
                url, json=venta, headers=header, verify=False)
            #print(venta)
            if response.status_code == 200:
                r_json=response.json()
                external_id=r_json['data']['external_id']
                update_anulados_pgsql(external_id, int(venta['id_venta']))
                print(response.content)
            else:
                print(response.status_code)
        #cont += 1

def create_notaCredito(header_dics):
    convenio = read_empresa_pgsql()
    url = convenio[1] + "/api/documents"
    token = convenio[0]
    _send_cpe_notaCredito(url, token, header_dics)

def _send_cpe_notaCredito(url, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    #cont = 0
    for venta in data:
        #if cont == 0:
            response = requests.post(
                url, json=venta, headers=header, verify=False)
            if response.status_code == 200:
                r_json=response.json()
                external_id=r_json['data']['external_id']
                update_notaCredito_pgsql(external_id, int(venta['id_venta']))
                print(response.content)
            else:
                print(response.status_code)
        #cont += 1

def create_resumen(header_dics, tipo):
    convenio = read_empresa_pgsql()
    url = convenio[1] + "/api/summaries"
    token = convenio[0]
    _send_cpe_resumen(url, token, header_dics, tipo)

def _send_cpe_resumen(url, token, data, tipo):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    for resumen in data:
            response = requests.post(
                url, json=resumen, headers=header, verify=False)
            if response.status_code == 200:
                r_json=response.json()
                external_id = r_json['data']['external_id']
                ticket = r_json['data']['ticket']
                if tipo == 'B':
                    insert_resumen_pgsql('B', ticket, external_id, resumen['fecha_de_emision_de_documentos'])
                elif tipo == 'A':
                    insert_resumen_pgsql('A', ticket, external_id, resumen['fecha_de_emision_de_documentos'])
                print(response.content)
            else:
                print(response.status_code)

def create_consulta(header_dics):
    convenio = read_empresa_pgsql()
    url = convenio[1] + "/api/summaries/status"
    token = convenio[0]
    _send_cpe_consulta(url, token, header_dics)

def _send_cpe_consulta(url, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    for consulta in data:
            response = requests.post(
                url, json=consulta, headers=header, verify=False)
            if response.status_code == 200:
                r_json=response.json()
                filename = r_json['data']['filename']
                external_id = r_json['data']['external_id']
                update_consulta_pgsql(consulta['id_resumen'], filename, external_id)
                print(response.content)
            else:
                print(response.status_code)