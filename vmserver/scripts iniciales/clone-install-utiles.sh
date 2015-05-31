#!/bin/bash
# My first script

mkdir /home/utnso/Escritorio
mkdir /home/utnso/Escritorio/git
cd /home/utnso/Escritorio/git
git clone https://github.com/sisoputnfrba/tp-2015-1c-dalemartadale.git /home/utnso/Escritorio/git

cd /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/vmserver/utiles/Debug
make
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/Debug


