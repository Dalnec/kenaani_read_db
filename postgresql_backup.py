
import gzip
import subprocess
from subprocess import PIPE, Popen

cmd = 'pg_dump -d BD_Carina -p 5432 -U postgres -F t -f D:\mybackups\mibase1.backup'
with gzip.open('backup.gz', 'wb') as f:
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)

for stdout_line in iter(popen.stdout.readline, ""):
    f.write(stdout_line.encode('utf-8'))

popen.stdout.close()
popen.wait()
