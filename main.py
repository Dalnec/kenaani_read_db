#import schedule
import time
import sys

if __name__ == "__main__":
    sys.path.append('kulami')
    sys.path.append('base')
    sys.path.append('pseapi')

    from kulami.models import leer_db_access
    from kulami.models_annulled import leer_db_fanulados, leer_db_banulados
    from pseapi.api import create_document, create_document_anulados
    
    while True:

        # seleccion boletas/facturas sin procesar kulami
        lista_ventas = leer_db_access()
        # generamos la lista y envio de documentos
        create_document(lista_ventas)
        print("provando env")
        time.sleep(5)
        #Seleccion de Documentos anuladas y envio
        lista_ventas_anulados = leer_db_fanulados()    
        create_document_anulados(lista_ventas_anulados, 1)
        lista_ventas_anulados = leer_db_banulados()
        create_document_anulados(lista_ventas_anulados, 3)
        time.sleep(5) 