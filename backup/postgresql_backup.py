
import gzip
import subprocess
from subprocess import PIPE, Popen
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

db_name = config['BASE']['DB_NAME']
bu_location = config['BACKUP']['BU_LOCATION']

def backup():
    cmd='pg_dump -d '+ db_name +' -p 5432 -U postgres -F t -f '+ bu_location
    with gzip.open('backup.gz', 'wb') as f:
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    
    for stdout_line in iter(popen.stdout.readline, ""):
        f.write(stdout_line.encode('utf-8'))
    
    popen.stdout.close()
    popen.wait()
