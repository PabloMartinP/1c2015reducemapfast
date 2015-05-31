#!/bin/bash

cp -a /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/vmserver/utiles/.  /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/

cd /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/Debug

make clean
make


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/Debug
echo "Export library path creado OK"

