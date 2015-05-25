/*
 * procesoFileSystem.h
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */

#ifndef PROCESOFILESYSTEM_H_
#define PROCESOFILESYSTEM_H_

#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include "consola.h"
#include "fileSystem.h"
//#include "directorios.h"
//#include "archivos.h"

//#include <util.h>

char FILE_CONFIG[1024]="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt";
char FILE_LOG[1024] ="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/log.txt";


t_log* logger;
t_config* config;

void print_directorio_actual();
void iniciar_consola();
void procesar_mensaje_nodo(int fd, t_msg* msg);
void inicializar();
void set_cwd();
void finalizar();
void iniciar_server_nodos_nuevos();


#endif /* PROCESOFILESYSTEM_H_ */
