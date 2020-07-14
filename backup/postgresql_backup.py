
import gzip
import subprocess
from subprocess import PIPE, Popen
import configparser
import time
import shutil
from backup.send_drive import searchFile

config = configparser.ConfigParser()
config.read('config.ini')

db_name = config['BASE']['DB_NAME']
bu_name = config['BACKUP']['BU_NAME']
db_drive = config['BACKUP']['BU_DRIVE']

file_name = bu_name +'.backup'
gz_name = file_name +'.gz'

def backup():
    cmd ='pg_dump -d '+ db_name +' -p 5432 -U postgres -F t -f '+ file_name
    #cmd = 'pg_dump -d '+ db_name +' -F t -f '+ file_name

    with gzip.open(file_name, 'wb') as f:
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)    
    for stdout_line in iter(popen.stdout.readline, ""):
        f.write(stdout_line.encode('utf-8'))    
    popen.stdout.close()
    popen.wait()
    
    with open(file_name, 'rb') as f_in:
        with gzip.open(gz_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    time.sleep(1)

    if db_drive:
        searchFile(100, "name = '"+gz_name+"'", gz_name)      
        



