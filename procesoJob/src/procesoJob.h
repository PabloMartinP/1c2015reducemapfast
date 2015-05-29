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

typedef struct {
	char ip[15];
	int puerto;
	int numero_bloque;
	char* archivo_tmp;
}t_map;

t_list* mappers;

int JOB_ID;
int conectar_con_marta();
t_list* recibir_mappers(int fd);

int conectar_con_marta(){
	int res, i;
	//conecto con marta
	printf("conectando con marta\n");
	int fd ;
	if((fd= client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA()))==-1){
		printf("No se pudo conectar con marta. MaRTA: %s:%d\n", JOB_IP_MARTA(), JOB_PUERTO_MARTA());
		handle_error("client_socket");
	}
	printf("Conectado\n");

	t_msg* msg;
	msg = string_message(JOB_HOLA, "hola soy un job",0);


	//envio el mensaje
	printf("enviando mensaje\n");
	res = enviar_mensaje(fd, msg);
	destroy_message(msg);
	if(res==-1){
		printf("no se pudo enviar mensaje a marta\n");
		handle_error("enviar_mensaje");
	}
	printf("mensaje enviado\n");

	printf("Esperando respuesta\n");
	//recibo la respuesta
	if((msg= recibir_mensaje(fd))==NULL){
		handle_error("recibir_mensaje");
	}
	printf("Mensaje recibido\n");
	//muestro el mensaje que me envio
	print_msg(msg);
	if(msg->header.id==MARTA_JOB_ID){
		printf("COnexion co marta OK\n");
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

	enviar_mensaje(fd, msg);
	//print_msg(msg);
	destroy_message(msg);

	//empiezo a enviar los archivos uno por uno
	for(i=0;archivos[i]!=NULL;i++){
		printf("archivo %d: %s\n", i, archivos[i]);
		msg = string_message(JOB_ARCHIVO, archivos[i], 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);
	}

	//libero archivos
	free_split(archivos);

	//hasta aca ya le envie los archivos a marta
	//ahora me tiene que contestar donde estan(nodo) y que blqoues para lanzar los mappers

	//primero me manda la cantidad de mappers
	mappers = recibir_mappers(fd);





	return 0;
}

t_list* recibir_mappers(int fd){
	t_list* lista = list_create();
	t_msg* msg;
	msg = recibir_mensaje(fd);
	print_msg(msg);
	int cant_mappers = msg->argv[0];
	destroy_message(msg);
	int i;
	t_map* map = NULL;
	for (i = 0; i < cant_mappers; i++) {

		map = malloc(sizeof(t_map));
		//recibo primero el nombre del archivo que va usar para guardar en el tmp del nodo
		msg = recibir_mensaje(fd);
		print_msg(msg);
		map->archivo_tmp = string_new();
		string_append(&(map->archivo_tmp), msg->stream);
		destroy_message(msg);

		//recibo el ip, puerto y nrobloque
		msg = recibir_mensaje(fd);
		print_msg(msg);
		strcpy(map->ip, msg->stream);
		map->puerto = msg->argv[0];
		map->numero_bloque = msg->argv[1];
		destroy_message(msg);

		list_add(lista, (void*)map);
	}

	return lista;
}


#endif /* PROCESOJOB_H_ */
