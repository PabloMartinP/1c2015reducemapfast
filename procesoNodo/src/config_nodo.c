/*
 * config_nodo.c
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */

#include "config_nodo.h"


void NODO_CONFIG_INIT(){
	strcpy(DIR_TMP, config_get_string_value(config, "DIR_TEMP"));
	strcpy(IP_NODO, config_get_string_value(config, "IP_NODO"));
	PUERTO_NODO = config_get_int_value(config, "PUERTO_NODO");;

	strcpy(IP_FS, config_get_string_value(config, "IP_FS"));
	PUERTO_FS = config_get_int_value(config, "PUERTO_FS");;

	ID = config_get_int_value(config, "ID");;
}

int NODO_PORT_FS(){
	return PUERTO_FS;
	//return config_get_int_value(config, "PUERTO_FS");
}
int NODO_ID(){
	return ID;
	//return config_get_int_value(config, "ID");
}
char* NODO_IP_FS(){
	return IP_FS;
	//return config_get_string_value(config, "IP_FS");
}

int NODO_TAMANIO_DATA_DEFAULT_MB(){
	return config_get_int_value(config, "SIZE_DATA_DEFAULT_MB");
}
char* NODO_ARCHIVOBIN(){
	return config_get_string_value(config, "ARCHIVO_BIN");
}
bool NODO_NUEVO(){
	return string_equals_ignore_case(config_get_string_value(config, "NODO_NUEVO"), "SI");
}
int NODO_PORT(){
	return PUERTO_NODO;
	//return config_get_int_value(config, "PUERTO_NODO");
}
char* NODO_IP(){
	return IP_NODO;
	//return config_get_string_value(config, "IP_NODO");
	//return ip_get();
}
char* NODO_DIRTEMP(){
	return DIR_TMP;
	//return config_get_string_value(config, "DIR_TEMP");
}
