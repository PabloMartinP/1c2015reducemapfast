#!/bin/bash
# My first script

cd /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/vmserver/utiles/Debug

cp -a Escritorio/git/tp-2015-1c-dalemartadale/vmserver/utiles/.  Escritorio/git/tp-2015-1c-dalemartadale/utiles/

make


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/Debug
echo "Export library path creado OK"

