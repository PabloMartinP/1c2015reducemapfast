/*
 ============================================================================
 Name        : procesoMaRTA.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <util.h>

#include "procesos.h"



int main(void) {
	config = config_create(FILE_CONFIG);

	int tamanio = 0;
	int cantidad = 0;
	bool soporta;
	t_list* lista;

	iniciar_server_MaRTA();

	// socket (JOB_LIBRE);//

	tamanio = archivo_a_usar();

	lista = nodos_usados();

	cantidad = hilos_de_mapper();		//PLANIFICACIÓN DE RUTINA DE MAPP//

	/* SOCKET (JOB_MAPPEANDO);
	SOCKET (JOB_TERMINO); */

	soporta = soporta_combiner();

	if (soporta == true)
		rutina_con_combiner();		//PLANIFICACIÓN DE RUTINA DE REDUCE//
		else
		rutina_sin_combiner();		//PLANIFICACIÓN DE RUTINA DE REDUCE//

	enviar_rutina_reduce();

	/*SOCKET (JOB_REDUCIENDO);
	SOCKET (JOB_TERMINO);
	SOCKET (COPIA_RESULTADO_A_MDFS);
	SOCKET (COPIADO);
	SOCKET ((TERMINADO);
	lIBERAR_JOB();
	*/

	config_destroy(config);
	return EXIT_SUCCESS;
}