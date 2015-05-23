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

char FILE_CONFIG[1024]="/config.txt";
char FILE_LOG[1024] ="/log.txt";

//char CWD[PATH_MAX_LEN];//para guardar el currentworkingdirectory


t_log* logger;
t_config* config;


void iniciar_consola();
void procesar_mensaje_nodo(int fd, t_msg* msg);
void inicializar();
void finalizar();
void iniciar_server_nodos_nuevos();


#endif /* PROCESOFILESYSTEM_H_ */
