#!/bin/bash

cp -a /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/vmserver/procesoFileSystem/.  /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/

cd /home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/Debug

make clean
make

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/utiles/Debug

echo "Export library path creado OK"
echo "FileSystemFileSystemFileSystemFileSystemFileSystemFileSystemFileSystemFileSystem"

