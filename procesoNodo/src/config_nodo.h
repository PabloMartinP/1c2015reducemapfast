/*
 * config_nodo.h
 *
 *  Created on: 29/4/2015
 *      Author: utnso
 */

#ifndef CONFIG_NODO_H_
#define CONFIG_NODO_H_

#include <commons/config.h>
#include <util.h>

int NODO_PORT_FS();
char* NODO_IP_FS();
char* NODO_ARCHIVOBIN();
int NODO_PORT();
char* NODO_IP();
char* NODO_DIRTEMP();
bool NODO_NUEVO();
int NODO_ID();
int NODO_TAMANIO_DATA_DEFAULT_MB();

/*
 * DEFINICIONES
 */

t_config* config;

#endif /* CONFIG_NODO_H_ */
