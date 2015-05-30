/*
 * filesystem_config.h
 *
 *  Created on: 30/5/2015
 *      Author: utnso
 */

#ifndef FILESYSTEM_CONFIG_H_
#define FILESYSTEM_CONFIG_H_

#include <commons/config.h>

char FILE_CONFIG[1024]="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt";
t_config* config;

char* FILESYSTEM_FILE_DIRECTORIOS();
int FILESYSTEM_CANTIDAD_MININA_NODOS();


int FILESYSTEM_CANTIDAD_MININA_NODOS(){
	return config_get_int_value(config, "CANT_NODOS_MINIMA");
}

char* FILESYSTEM_FILE_DIRECTORIOS(){
	return config_get_string_value(config, "DIRECTORIOS");
}
char* FILESYSTEM_FILE_ARCHIVOS(){
	return config_get_string_value(config, "ARCHIVOS");
}
char* FILESYSTEM_FILE_BLOQUES(){
	return config_get_string_value(config, "BLOQUES");
}


#endif /* FILESYSTEM_CONFIG_H_ */
