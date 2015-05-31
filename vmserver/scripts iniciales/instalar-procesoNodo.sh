#!/bin/bash

cp -a /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/vmserver/procesoNodo/.  /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/

cd /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/Debug

make clean
make

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/Debug

echo "Export library path creado OK"
echo "procesoNodo123"

