/*
 * procesos.h
 *
 *  Created on: 17/5/2015
 *      Author: utnso
 */

#ifndef PROCESOJOB_H_
#define PROCESOJOB_H_

#include <stdio.h>
#include "config_job.h"
#include <commons/collections/list.h>
#include <pthread.h>

#include <libgen.h>
#include <stdlib.h>
#include <commons/log.h>

#include <nodo.h>
#include "config_job.h"

#include <util.h>

/*
typedef struct{
	int id;
	char resultado[255];
	t_list* nodos_archivo;//list of t_nodo_archivo
}t_reduce_job;
*/

t_list* mappers;
t_list* reducers;
t_log* logger;
pthread_mutex_t mutex_log;

int JOB_ID;
int conectar_con_marta();
t_list* recibir_mappers(int fd);

int lanzar_hilos_mappers(int fd);
void crearHiloMapper();
int funcionMapping(t_map* map);
//void reduce_job_free(t_reduce_job* job);
int lanzar_hilo_reduce(int fd, t_reduce* reduce);

int funcionReducing(t_reduce* reduce);


int funcionReducing(t_reduce* reduce){
	bool resultado;

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Conectando con nodo %s", nodo_base_to_string(reduce->nodo_base_destino));
	pthread_mutex_unlock(&mutex_log);

	/*
	strcpy(reduce->nodo_base_destino->red.ip, "192.168.1.43");
		reduce->nodo_base_destino->red.puerto = 6001;
		reduce->nodo_base_destino->id = 43;

	void __test(t_nodo_archivo* na){
		strcpy(na->nodo_base->red.ip, "192.168.1.43");
		na->nodo_base->red.puerto = 6001;
		na->nodo_base->id = 43;
	}
	list_iterate(reduce->nodos_archivo, (void*)__test);
*/

	int fd = client_socket(reduce->nodo_base_destino->red.ip, reduce->nodo_base_destino->red.puerto);
	t_msg* msg;

	//envio esto asi entra en el select del reduce
	msg = argv_message(JOB_REDUCER, 0);
	enviar_mensaje(fd, msg);destroy_message(msg);


	enviar_mensaje_reduce(fd, reduce);
	enviar_mensaje_script(fd, JOB_SCRIPT_REDUCER());

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Esperando respuesta a reducer %d en nodo %s",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
	pthread_mutex_unlock(&mutex_log);


	msg = recibir_mensaje(fd);

	if(msg==NULL){
		printf("ERRRRRRRoor reduce\n");

	}


	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Respuesta recibida de reducer %d en nodo %s",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));

	if(msg->header.id==REDUCER_TERMINO){
		log_trace(logger, "El reducer %d en nodo %s termino OK", reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
		resultado = true;
	}
	else{
		log_trace(logger, "El reducer %d en nodo %s Termino CON ERRORES",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
		resultado = false;
	}
	pthread_mutex_unlock(&mutex_log);

	destroy_message(msg);

	close(fd);

	return resultado;
}


int funcionMapping(t_map* map){
	t_msg* msg;
	bool resultado;
	pthread_mutex_lock(&mutex_log);


	/*strcpy(map->archivo_nodo_bloque->base->red.ip, "192.168.1.43");
	map->archivo_nodo_bloque->base->red.puerto = 6001;
	map->archivo_nodo_bloque->base->id = 43;*/

	log_trace(logger, "Conectando con nodo %d - %s:%d", map->archivo_nodo_bloque->base->id,  map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	pthread_mutex_unlock(&mutex_log);
	int fd = client_socket(map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	printf("nuevo Sock %d\n", fd);
	//envio esto para que entre en el select del MAPPER y espera que recibir un mensaje_map
	msg = argv_message(JOB_MAPPER, 0);
	enviar_mensaje(fd, msg);destroy_message(msg);

	enviar_mensaje_map(fd, map);

	enviar_mensaje_script(fd, JOB_SCRIPT_MAPPER());


	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Esperando respuesta sock:%d a mapper %d en nodo %s:%d",fd, map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	pthread_mutex_unlock(&mutex_log);

	char buffer[32];
	if (recv(fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT) == 0) {
		printf("El  NODOOOO el sock %d conexionNNNNNNNNNNNNNNNNNNNNN \n", fd);
		// if recv returns zero, that means the connection has been closed:

		close(fd);
		return -1;
		// do something else, e.g. go on vacation
	}else
		printf("sock %d activo??????????\n", fd)	;

	//pthread_mutex_lock(&mutex_log);
	printf("Esperando a que le contesnte T_T sock %d map %d\n", fd, map->info->id);
	/*
	while((msg = recibir_mensaje(fd))==NULL){
		//if nothing was received then we want to wait a little before trying again, 0.1 seconds
		printf("sock cerrado %d map %d\n", fd, map->info->id);
		usleep(1000000);
	}*/
	msg = recibir_mensaje(fd);
	if(msg!=NULL)
		printf("CONTESTARONNNNNNNNNNNNNNNNN sock %d map %d \n", fd, map->info->id);
	else
		printf("NOOOOOOOOOOOOOOOOOOOOOOOOOOOsock %d map %d \n", fd, map->info->id);
	//pthread_mutex_unlock(&mutex_log);

	if(msg==NULL){
		printf("rec msg NULLLLLLLLLL sock %d, map: %d\n", fd, map->info->id);

	}
	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Respuesta recibida de mapper %d en nodo %s:%d",map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	if(msg->header.id==MAPPER_TERMINO){
		log_trace(logger, "El mapper %d en nodo %s:%d termino OK", map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
		resultado = true;
	}
	else{
		log_trace(logger, "El mapper %d en nodo %s:%d Termino CON ERRORES",map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
		resultado = false;
	}
	pthread_mutex_unlock(&mutex_log);
	destroy_message(msg);
	printf("NINNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN\n");
	//enviar_mensaje_nodo_ok(fd);

	close(fd);

	return resultado;
}

void crearHiloMapper(t_map* map){
	pthread_t idHilo;

	log_trace(logger, "Lanzando hilo mapper");
	pthread_create(&idHilo, NULL, (void*)funcionMapping, (void*)map); //que párametro  ponemos??
	pthread_join(idHilo, NULL); //el proceso espera a que termine el hilo

}


int conectar_con_marta(){
	int res, i;
	//conecto con marta
	log_trace(logger, "Conectando con marta");
	int fd ;
	if((fd= client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA()))==-1){
		log_trace(logger, "No se pudo conectar con marta. MaRTA: %s:%d\n", JOB_IP_MARTA(), JOB_PUERTO_MARTA());
		handle_error("client_socket");
	}
	log_trace(logger, "Conectado con marta OK");

	t_msg* msg;
	msg = argv_message(JOB_HOLA, 0);
	//envio el mensaje
	res = enviar_mensaje(fd, msg);
	destroy_message(msg);
	if(res==-1){
		log_trace(logger, "no se pudo enviar mensaje a marta");
		handle_error("enviar_mensaje");
	}
	//recibo la respuesta
	if((msg= recibir_mensaje(fd))==NULL){
		handle_error("recibir_mensaje");
	}

	if(msg->header.id==MARTA_JOB_ID){
		log_info(logger, "ID Job asignado por marta: %d", JOB_ID);
		JOB_ID = msg->argv[0];//aca esta el id que asigno marta
	}
	destroy_message(msg);

	/*
	 * si todo salio bien hasta aca lo siguiente que hay que hacer es mandarle a marta los archivos a reducir
	 * y el nombre del archivo resultado, ademas si soporta o no combiner
	 * marta me contesta diciendome los nodos (ip:puerto) y numero_bloque del nodo de los archivos(los que selecciono de la planificacion)
	 * luego tengo que lanzar los hilos que se conecten al nodo, apliquen map, me avisen y yo le aviso a marta con el mensaje JOB_MAP_TERMINO
	 *
	 * por ejemplo si me llegan tres nodos son tres hilos mapper
	 * el proceso job se tiene que quedar bloqueado hasta que terminen los tres hilos (pthread_join()) (creo ...)
	 *
	 * una vez que terminaron los mappers tengo que esperar el mensaje de marta diciendome que archivos son los que tengo qeu reducir
	 * me va llegar algo asi como lista_archivos (guardados en el tmp del nodo) y el nodo(ip:puerto)
	 * creo tantos hilos reducers como nodos me lleguen
	 *
	 * finalmente puedo quedarme esperando a que marta me mande el mensaje JOB_TERMINADO
	 * para saber si termino correctamente
	 */

	//envio los archivos a marta
	char** archivos = JOB_ARCHIVOS();

	//envio la cantidad de archivos que son
	int cant_archivos = split_count(archivos);
	//paso si es combiner o no, el archivo destino del resultado yla cantidad de archivos a procesar
	msg = string_message(JOB_INFO, JOB_RESULTADO(), 2, JOB_COMBINER(), cant_archivos);
	log_trace(logger, "Enviando archivos a marta para que me devuelva los bloques donde lanzar los hilos mappers");
	log_trace(logger, "CantArchivos a enviar: %d", cant_archivos);
	enviar_mensaje(fd, msg);
	//print_msg(msg);
	destroy_message(msg);

	//empiezo a enviar los archivos uno por uno
	for(i=0;archivos[i]!=NULL;i++){
		log_trace(logger, "Archivo %d: %s\n", i, archivos[i]);
		msg = string_message(JOB_ARCHIVO, archivos[i], 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);
	}
	log_trace(logger, "Fin envio archivos");

	//libero archivos
	free_split(archivos);

	//hasta aca ya le envie los archivos a marta
	//ahora me tiene que contestar donde estan(nodo) y que blqoues para lanzar los mappers
	log_trace(logger, "*****************************************************");
	//primero me manda la cantidad de mappers
	log_trace(logger, "Comienzo a recibir los nodos-bloque donde lanzar los mappers");
	mappers = recibir_mappers(fd);
	log_trace(logger, "Fin recepcion de nodos-bloque para mappers");


	lanzar_hilos_mappers(fd);


	t_reduce* reduce = NULL;

	for(;;){
		//recibo un reduce
		reduce = recibir_mensaje_reduce(fd);

		if(reduce!=NULL){
			//ahora me queda enviarselos al nodo
			lanzar_hilo_reduce(fd, reduce);
			if(reduce->final){
				//es el reduce final, hago break y me rajo
				log_trace(logger, "**************************************************");
				log_trace(logger, "Archivo final %s generado en %s", reduce->info->resultado, nodo_base_to_string(reduce->nodo_base_destino));
				break;
			}

		}else{
			break;//hubo un error del reduce
		}
	}
	log_trace(logger, "Sali del for por algo");

	msg = argv_message(JOB_TERMINO, 1, JOB_ID);
	enviar_mensaje(fd, msg);
	destroy_message(msg);

	msg = argv_message(MARTA_SALIR, 0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);



	return 0;
}

int lanzar_hilo_reduce(int fd, t_reduce* reduce){
	log_trace(logger, "Comienzo a crear hilo reduce");

	pthread_t th;
	pthread_create(&th, NULL, (void*)funcionReducing, (void*)reduce); //que párametro  ponemos??

	int* res_reduce;
	pthread_join (th, (void**)&res_reduce);

	log_trace(logger, "Le aviso a Marta que el reduce %d finalizo", reduce->info->id);
	//le tengo que avisar a marta que termino el reduce ya sea bien o mal
	//paso el job_id asignado y el resultado y el map_id
	t_msg* msg = argv_message(JOB_REDUCE_TERMINO, 3, JOB_ID, res_reduce, reduce->info->id);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	log_trace(logger, "Mensaje enviado a marta con resultado: %d del reduce %d", res_reduce, reduce->info->id);




	log_trace(logger, "Fin de hilo reduce");
	return 0;
}


/*
void reduce_job_free(t_reduce_job* job){
	list_destroy_and_destroy_elements(job->nodos_archivo, (void*)nodo_archivo_destroy);
	FREE_NULL(job);
}
*/

/*
int recibir_reducers(int fd){
	t_msg* msg;
	t_nodo_archivo* na;


	t_nodo_base* nb;
	int cant_reduces,i;

	do{
		msg = recibir_mensaje(fd);
		if(msg->header.id==MARTA_REDUCE_INFO){
			//paso ip, puerto, id_nodo, cant_nodos_archivos
			nb = nodo_base_new(msg->argv[1], msg->stream, msg->argv[0]);
			cant_reduces = msg->argv[2];
		}else{
			//si marta me manda el fin reduces salgo del while porque ya termino de enviar reduces
			if(msg->header.id == MARTA_FIN_REDUCES)
				break;
		}
		destroy_message(msg);

		//guardo t_nodo_archivo
		reducers = list_create();

		log_trace(logger, "Comenzando a recibir los nodos-archivo. cant: %d", cant_reduces);
		for(i=0;i<cant_reduces;i++){
			na = nodo_archivo_create();

			//cargo el nombre del archivo a reducir
			msg = recibir_mensaje(fd);
			if(msg->header.id == MARTA_REDUCE_NOMBRE_TMP){
				strcpy(na->archivo, msg->stream);
			}
			destroy_message(msg);

			//cargo la info del nodo a donde conectarse
			////ip, puerto, id_nodo
			msg = recibir_mensaje(fd);
			if(msg->header.id == MARTA_REDUCE_NODO){
				na->nodo_base= nodo_base_new(msg->argv[1], msg->stream, msg->argv[0]);
			}
			destroy_message(msg);

			//agrego el reduce a la lista de reduce
			log_trace(logger, "recibido nuevo reduce sobre %s en el nodo_id: %d %s:%d", na->archivo, na->nodo_base->id, na->nodo_base->red.ip, na->nodo_base->red.puerto);
			list_add(reducers, (void*)na);
		}

		log_trace(logger, "Fin recepcion de nodos-archivo");

		//recibo el nombre donde guarda el archivo final y el REDUCE_ID
		t_reduce_job* reduce;
		msg = recibir_mensaje(fd);
		if(msg->header.id == MARTA_REDUCE_RESULTADO){
			reduce = malloc(sizeof(t_reduce_job));
			strcpy(reduce->resultado, msg->stream);
			reduce->id = msg->argv[0];

			log_trace(logger, "reduce_id: %d, guardar resultado en %s", reduce->id, reduce->resultado);
		}
		destroy_message(msg);

		log_trace(logger, "Fin envio recepcion de reduce id %d", reduce->id);

		/////////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////




	}while(true);

	return 0;
}
*/

int lanzar_hilos_mappers(int fd){
	//creo los hilos mappers
		log_trace(logger, "Comienzo a crear hilos mappers");
		pthread_t *threads = malloc(list_size(mappers)*(sizeof(pthread_t)));
		int i=0;
		void _crear_hilo_mapper(t_map* map){
			pthread_create(threads + i, NULL, (void*)funcionMapping, (void*)map);
			i++;
		}
		list_iterate(mappers, (void*)_crear_hilo_mapper);
		i=0;
		int* res_map = malloc(sizeof(int)*list_size(mappers));
		void _join_hilo_mapper(t_map* map){
			if (pthread_join (threads[i], (void**)res_map+i))
			   printf("Error mapper!!!!!!!!!!!!!!!!!\n");


			log_trace(logger, "Le aviso a Marta que el map %d finalizo", map->info->id);
			//le tengo que avisar a marta que termino el map ya sea bien o mal
			//paso el job_id asignado y el resultado y el map_id
			t_msg* msg = argv_message(MAPPER_TERMINO, 3, JOB_ID, res_map[i], map->info->id);
			enviar_mensaje(fd, msg);
			destroy_message(msg);
			log_trace(logger, "Mensaje enviado a marta con resultado: %d del map %d", res_map[i], map->info->id);


			i++;

			//libero el map
			//archivo_nodo_bloque_destroy_free_base(map->archivo_nodo_bloque);
			//map_free(map);

		}
		list_iterate(mappers, (void*)_join_hilo_mapper);
		free(threads);threads=NULL;
		free(res_map);res_map = NULL;

		log_trace(logger, "Fin de creacion de hilos mappers");

		list_destroy(mappers);

		return 0;
}

t_list* recibir_mappers(int fd){
	t_list* lista = list_create();
	t_msg* msg;
	msg = recibir_mensaje(fd);
	//print_msg(msg);
	int cant_mappers = msg->argv[0];
	log_trace(logger, "Cant. Mappers: %d", cant_mappers);
	destroy_message(msg);
	int i;
	t_map* map = NULL;
	for (i = 0; i < cant_mappers; i++) {
		log_trace(logger, "Mapper %d", i);

		map = recibir_mensaje_map(fd);

		print_map(map);

		log_trace(logger, "Map_id: %d - Ubicacion: id_nodo: %d, %s:%d numero_bloque: %d",map->info->id, map->archivo_nodo_bloque->base->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto, map->archivo_nodo_bloque->numero_bloque);


		list_add(lista, (void*)map);
	}

	return lista;
}


#endif /* PROCESOJOB_H_ */
