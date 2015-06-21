/*
 ============================================================================
 Name        : procesoMaRTA.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "procesoMaRTA.h"

int main(void) {
	config = config_create(FILE_CONFIG);
	marta_create();
	logger = log_create(FILE_LOG, "MaRTA", true, LOG_LEVEL_TRACE);


	iniciar_thread_server_MaRTA();
	//iniciar_servidor();//bloqueante, para no bloquear usar a iniciar_thread_server_MaRTA()


	while(!FIN);

	marta_destroy();
	log_destroy(logger);
	config_destroy(config);

	return EXIT_SUCCESS;
}
