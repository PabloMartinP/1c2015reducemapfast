/*
 * procesos.h
 *
 *  Created on: 17/5/2015
 *      Author: utnso
 */

#ifndef PROCESOJOB_H_
#define PROCESOJOB_H_

#include <util.h>
#include <stdio.h>
#include "config_job.h"
#include <commons/collections/list.h>
#include <pthread.h>

#include <libgen.h>
#include <stdlib.h>
#include <commons/log.h>


#include "config_job.h"


typedef struct {
	char ip[15];
	int puerto;
	int numero_bloque;
	char* archivo_tmp;
}t_map;

t_list* mappers;
t_log* logger;

int JOB_ID;
int conectar_con_marta();
t_list* recibir_mappers(int fd);
void crearHiloMapper();
int funcionMapping(t_map* map);
int enviar_script_mapper(int fd);

int funcionMapping(t_map* map){
	bool resultado ;
	//printf("%s:%d\n", map->ip, map->puerto);
	log_trace(logger, "Conectando con nodo %s:%d", map->ip, map->puerto);
	int fd = client_socket(map->ip, map->puerto);
	t_msg* msg;
	size_t tam = file_get_size(JOB_SCRIPT_MAPPER());
	//envio el bloque a mapear y el nombre del archivo donde almacena el resultado y al final el tamaño del script
	log_trace(logger, "Enviando %s, numero_bloque: %d", map->archivo_tmp, map->numero_bloque);
	msg = string_message(JOB_MAPPER, map->archivo_tmp, 2, map->numero_bloque, tam);
	enviar_mensaje(fd, msg);
	destroy_message(msg);

	enviar_script_mapper(fd);
	log_trace(logger, "Esperando respuesta a mapper en nodo %s:%d", map->ip, map->puerto);
	msg = recibir_mensaje(fd);
	log_trace(logger, "Respuesta recibida de mapper en nodo %s:%d", map->ip, map->puerto);
	if(msg->header.id==MAPPER_TERMINO){
		log_trace(logger, "El mapper en nodo %s:%d termino OK", map->ip, map->puerto);
		resultado = true;
	}
	else{
		log_trace(logger, "El mapper en nodo %s:%d Termino CON ERRORES", map->ip, map->puerto);
		resultado = false;
	}

	close(fd);

	log_trace(logger, "Le aviso a Marta que el job finalizo");
	//le tengo que avisar a marta que termino el map ya sea bien o mal
	fd = client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA());
	//paso el job_id asignado y el resultado
	msg = argv_message(MAPPER_TERMINO, 2, JOB_ID, resultado);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	log_trace(logger, "Mensaje enviado a marta con resultado: %d", resultado);

	close(fd);
	return 0;
}

int enviar_script_mapper(int fd){

	char* script = file_get_mapped(JOB_SCRIPT_MAPPER());

	//size_t tam = file_get_size(JOB_SCRIPT_MAPPER());
	//enviar_mensaje_sin_header(fd, tam, script);
	t_msg* msg = string_message(JOB_MAPPER, script, 0);
	log_trace(logger, "Enviando script mapper");
	enviar_mensaje(fd, msg);

	file_mmap_free(script, JOB_SCRIPT_MAPPER());
	return 0;
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

	//primero me manda la cantidad de mappers
	log_trace(logger, "Comienzo a recibir los nodos-bloque donde lanzar los mappers");
	mappers = recibir_mappers(fd);
	log_trace(logger, "Fin recepcion de nodos-bloque para mappers");

	//creo los hilos mappers
	log_trace(logger, "Comienzo a crear hilos mappers");
	pthread_t *threads = malloc(list_size(mappers)*(sizeof(pthread_t)));
	i=0;
	void _crear_hilo_mapper(t_map* map){
		pthread_create(threads + i, NULL, (void*)funcionMapping, (void*)map); //que párametro  ponemos??
		i++;
	}
	list_iterate(mappers, (void*)_crear_hilo_mapper);
	i=0;
	int* res_map = malloc(sizeof(int)*list_size(mappers));
	void _join_hilo_mapper(t_map* map){
		if (pthread_join (threads[i], (void**)res_map+i))
		   printf("Error mapper!!!!!!!!!!!!!!!!!\n");

		printf("Resultado Map=%d", res_map[i]);
		i++;
	}
	list_iterate(mappers, (void*)_join_hilo_mapper);
	free(threads);threads=NULL;
	free(res_map);res_map = NULL;

	log_trace(logger, "Fin de creacion de hilos mappers");


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
		map = malloc(sizeof(t_map));
		//recibo primero el nombre del archivo que va usar para guardar en el tmp del nodo
		msg = recibir_mensaje(fd);
		//print_msg(msg);
		map->archivo_tmp = string_new();
		string_append(&(map->archivo_tmp), msg->stream);
		log_trace(logger, "Nombre archivo donde guardar el resultado(en tmp del nodo): %s", msg->stream);
		destroy_message(msg);

		//recibo el ip, puerto y nrobloque
		msg = recibir_mensaje(fd);
		print_msg(msg);
		strcpy(map->ip, msg->stream);
		map->puerto = msg->argv[0];
		map->numero_bloque = msg->argv[1];
		log_trace(logger, "Ubicacion: %s:%d numero_bloque: %d", map->ip, map->puerto, map->numero_bloque);
		destroy_message(msg);

		list_add(lista, (void*)map);
	}

	return lista;
}


#endif /* PROCESOJOB_H_ */
