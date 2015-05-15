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
/*
 * DEFINICIONES
 */
t_config* config =NULL;

int NODO_PORT_FS(){
	return config_get_int_value(config, "PUERTO_FS");
}
char* NODO_IP_FS(){
	return config_get_string_value(config, "IP_FS");
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
	//return config_get_string_value(config, "IP_NODO");
	return ip_get();
}
char* NODO_DIRTEMP(){
	return config_get_string_value(config, "DIR_TEMP");
}

#endif /* CONFIG_NODO_H_ */
