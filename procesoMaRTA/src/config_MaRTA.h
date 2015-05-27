/*
 * config_MaRTA.h
 *
 *  Created on: 17/5/2015
 *      Author: utnso
 */

#ifndef CONFIG_MARTA_H_
#define CONFIG_MARTA_H_


#include <commons/config.h>

char FILE_CONFIG[1024]="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoMaRTA/config.txt";
t_config* config;

int MaRTA_PUERTO();
char* MaRTA_IP();
char* MaRTA_IP_FS();
int MaRTA_PUERTO_FS();

char* MaRTA_IP_FS(){
	return config_get_string_value(config, "IP_FS");
}
int MaRTA_PUERTO_FS(){
	return config_get_int_value(config, "PUERTO_FS");
}

int MaRTA_PUERTO(){
	return config_get_int_value(config, "PUERTO");
}
char* MaRTA_IP(){
	return config_get_string_value(config, "IP");
}

#endif /* CONFIG_MARTA_H_ */
