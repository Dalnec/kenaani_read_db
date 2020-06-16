import time
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
db_state = config['BACKUP']['BU_STATE']
db_time = config['BACKUP']['BU_TIME']

time_doc = config['MAIN']['M_TIMEBF']
time_anul = config['MAIN']['M_TIMEANUL']
time_notaC = config['MAIN']['M_TIMENC']

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')
    sys.path.append('backup')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from kulami.models_notaCredito import leer_db_notaCredito
    from pseapi.api import create_document, create_anulados, create_notaCredito
    from backup.postgresql_backup import backup
    
    while True:
        named_tuple = time.localtime()
        time_string = time.strftime("%H:%M:%S", named_tuple)
        if time_string == time_doc:
            print("Enviando...")
            # seleccion boletas/facturas sin procesar kulami
            lista_ventas = leer_db_access()
            # generamos la lista y envio de documentos
            create_document(lista_ventas)
            #time.sleep(1)
        
        elif time_string == time_anul:
            print("Anulados...")
            #Seleccion de Documentos anuladas y envio
            lista_anulados = leer_db_fanulados()    
            create_anulados(lista_anulados, 1)
            lista_anulados = leer_db_banulados()
            create_anulados(lista_anulados, 3)
            #time.sleep(1)
            
        elif time_string == time_notaC:
            print("Notas Creditos...")
            lista_notaCredito = leer_db_notaCredito()
            create_notaCredito(lista_notaCredito)            

        elif time_string == db_time and db_state:
            print("Backups...")
            backup()
        time.sleep(1)