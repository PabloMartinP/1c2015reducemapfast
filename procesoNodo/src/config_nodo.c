/*
 * config_nodo.c
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */

#include "config_nodo.h"



int NODO_PORT_FS(){
	return config_get_int_value(config, "PUERTO_FS");
}
int NODO_ID(){
	return config_get_int_value(config, "ID");
}
char* NODO_IP_FS(){
	return config_get_string_value(config, "IP_FS");
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
	return config_get_int_value(config, "PUERTO_NODO");
}
char* NODO_IP(){
	return config_get_string_value(config, "IP_NODO");
	//return ip_get();
}
char* NODO_DIRTEMP(){
	return config_get_string_value(config, "DIR_TEMP");
}
