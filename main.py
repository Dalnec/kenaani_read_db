import time
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
db_state = config['BACKUP']['BU_STATE']
db_time = config['BACKUP']['BU_TIME']

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')
    sys.path.append('backup')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from pseapi.api import create_document, create_document_anulados
    from backup.postgresql_backup import backup
    #from backup.send_drive import imprimir, uploadFile
    
    while True:
        named_tuple = time.localtime()
        time_string = time.strftime("%H:%M:%S", named_tuple)
        if time_string == '10:03:00':
            # seleccion boletas/facturas sin procesar kulami
            lista_ventas = leer_db_access()
            # generamos la lista y envio de documentos
            create_document(lista_ventas)
            print("provando env")
            time.sleep(5)

        elif time_string == '10:05:00':
            #Seleccion de Documentos anuladas y envio
            lista_ventas_anulados = leer_db_fanulados()    
            create_document_anulados(lista_ventas_anulados, 1)
            lista_ventas_anulados = leer_db_banulados()
            create_document_anulados(lista_ventas_anulados, 3)
            print("anulados")
            time.sleep(5)

        #elif db_state and db_time == time_string:
        backup()
        time.sleep(5) 