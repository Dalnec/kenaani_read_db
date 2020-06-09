import requests

from base.db import (
    read_empresa_pgsql,
    update_venta_pgsql,
    update_venta_anulados_pgsql
)
#from cafaedb.models import update_enviado
from urllib3.exceptions import InsecureRequestWarning

# Disable flag warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def create_document(header_dics):
    "crea boletas y facturas"
    convenio = read_empresa_pgsql()
    url = convenio[15] + "/api/documents"
    token = convenio[14]
    _send_cpe(url, token, header_dics)


def _send_cpe(url, token, data):
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
                update_venta_pgsql(external_id, int(venta['id_venta']))
                print(response.content)
            else:
                print(response.status_code)
        #cont += 1


def create_document_anulados(header_dics, tipo):
    "crea facturas y boletas anuladas"
    convenio = read_empresa_pgsql()
    if tipo == 1 :
        url = convenio[15] + "/api/voided"
    else:
        url = convenio[15] + "/api/summaries"
    token = convenio[14]
    _send_cpe_anulados(url, token, header_dics)


def _send_cpe_anulados(url, token, data):
    header = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }
    cont = 0
    for venta in data:
        if cont == 0:
            response = requests.post(
                url, json=venta, headers=header, verify=False)
            
            if response.status_code == 200:
                r_json=response.json()
                external_id=r_json['data']['external_id']
                update_venta_anulados_pgsql(external_id, int(venta['id_venta']))
                print(response.content)
            else:
                print(response.status_code)
        cont += 1
