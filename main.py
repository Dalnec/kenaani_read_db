import time
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
db_state = config['BACKUP']['BU_STATE']
db_time = config['BACKUP']['BU_TIME']
db_time2 = config['BACKUP']['BU_TIME2']

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
    #from kulami.models_resumen import leer_db_resumen, leer_db_consulta
    from pseapi.api import create_document, create_anulados, create_notaCredito, create_resumen, create_consulta
    from backup.postgresql_backup import backup

    def _get_time(format):
        timenow = time.localtime()
        if format == 1:
            timenow = time.strftime("%Y/%m/%d %H:%M:%S", timenow)
        else:
            timenow = time.strftime("%H:%M:%S", timenow)
        return timenow

    while True:
        try:
            print("Enviando...")
            lista_ventas = leer_db_access()
            create_document(lista_ventas)
            time.sleep(1)  
        except Exception as e:
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        # if time_string == time_doc:
        '''print("Resumenes...")
        resumen = leer_db_resumen()
        create_resumen(resumen, 'B')
        time.sleep(1)
        print("Consulta...")
        resumen = leer_db_consulta()
        create_consulta(resumen)'''
        try:
            print("Anulados Facturas...")
            lista_anulados = leer_db_fanulados()
            create_anulados(lista_anulados, 1)
            time.sleep(1)
        except Exception as e:
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)
        
        try:
            print("Anulados Boletas...")
            lista_anulados = leer_db_banulados()
            create_anulados(lista_anulados, 3)
            time.sleep(1)
        except Exception as e:
            print(_get_time(1) + ": {}".format(e))
            time.sleep(2)

        #if time_string == time_notaC:
        print("Notas Creditos...")
        lista_notaCredito = leer_db_notaCredito()
        create_notaCredito(lista_notaCredito)

        time_now = _get_time(2)
        if  time_now >= db_time and time_now <= db_time2 and db_state:
            try:
                print("Backups...")
                backup()
            except Exception as e:
                print(_get_time(1) + ": {}".format(e)) 
        time.sleep(1)