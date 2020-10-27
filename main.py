import time
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
db_state = eval(config['BACKUP']['BU_STATE'])
db_time = config['BACKUP']['BU_TIME']
db_time2 = config['BACKUP']['BU_TIME2']

state_doc = eval(config['MAIN']['M_DOC'])
state_anul = eval(config['MAIN']['M_ANUL'])
state_guia = eval(config['MAIN']['M_GUIA'])
time_notaC = config['MAIN']['M_TIMENC']
time_notaC2 = config['MAIN']['M_TIMENC2']

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')
    sys.path.append('backup')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from kulami.models_notaCredito import leer_db_notaCredito
    from kulami.models_guiaRemision import leer_db_guia
    from kulami.models_rechazados import leer_db_rechazados
    #from kulami.models_resumen import leer_db_resumen, leer_db_consulta
    from pseapi.api import create_document, create_anulados, create_notaCredito, create_resumen, create_consulta, create_guiaRemision, create_rechazados
    from backup.postgresql_backup import backup
    from base.db import _get_time

    while True:
        try:
            if state_doc:            
                lista_ventas = leer_db_access()
                create_document(lista_ventas)
                time.sleep(1)  
        except Exception as e:
            print("Enviando...")
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        try:
            if state_anul:                
                lista_rechazados = leer_db_rechazados()
                create_rechazados(lista_rechazados)
                time.sleep(1)
        except Exception as e:
            print("Anulados Rechazados...")
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        try:
            if state_anul:                
                lista_anulados = leer_db_fanulados()
                create_anulados(lista_anulados, 1)
                time.sleep(1)
        except Exception as e:
            print("Anulados Facturas...")
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        try:
            if state_anul:                
                lista_anulados = leer_db_banulados()
                create_anulados(lista_anulados, 3)
                time.sleep(1)
        except Exception as e:
            print("Anulados Boletas...")
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        time_now = _get_time(2)
        if  time_now >= time_notaC and time_now <= time_notaC2:        
            try:                
                lista_notaCredito = leer_db_notaCredito()
                create_notaCredito(lista_notaCredito)
            except Exception as e:
                print("Notas Creditos...")
                print(_get_time(1) + ": {}".format(e))
                time.sleep(2)

        try:
            if state_guia:
                lista_guia = leer_db_guia()
                create_guiaRemision(lista_guia)
        except Exception as e:
            print("Guia de Remision...")
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        time_now = _get_time(2)
        if  time_now >= db_time and time_now <= db_time2 and db_state:
            try:
                backup()
                time.sleep(1)
            except Exception as e:
                print(_get_time(1) + ": {}".format(e)) 
                time.sleep(1)

        # if time_string == time_doc:
        '''print("Resumenes...")
        resumen = leer_db_resumen()
        create_resumen(resumen, 'B')
        time.sleep(1)
        print("Consulta...")
        resumen = leer_db_consulta()
        create_consulta(resumen)'''