/*
 * config_job.h
 *
 *  Created on: 17/5/2015
 *      Author: utnso
 */

#ifndef CONFIG_JOB_H_
#define CONFIG_JOB_H_

#include <commons/config.h>
#include <stdlib.h>

char FILE_CONFIG [1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/jobConfig.txt";
t_config* jobConfig = NULL;

char* JOB_IP_MARTA();
char* JOB_RESULTADO();


char* JOB_IP_MARTA(){
	return config_get_string_value(jobConfig, "IP_MARTA");
}

char** JOB_ARCHIVOS(){
	return config_get_array_value(jobConfig, "ARCHIVOS");
}

char* JOB_RESULTADO(){
	return config_get_string_value(jobConfig, "RESULTADO");
}

int JOB_COMBINER(){
	return config_get_int_value(jobConfig, "COMBINER");
}

char* JOB_SCRIPT_MAPPER(){
	return config_get_string_value(jobConfig, "MAPPER");
}
char* JOB_SCRIPT_REDUCER(){
	return config_get_string_value(jobConfig, "REDUCE");
}

int JOB_PUERTO_MARTA(){
	return config_get_int_value(jobConfig, "PUERTO_MARTA");
}


#endif /* CONFIG_JOB_H_ */
