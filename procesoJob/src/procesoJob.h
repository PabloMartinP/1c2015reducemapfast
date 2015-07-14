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

#include <stdlib.h>         /* exit(), malloc(), free() */
#include <sys/types.h>      /* key_t, sem_t, pid_t      */
#include <sys/shm.h>        /* shmat(), IPC_RMID        */
#include <errno.h>          /* errno, ECHILD            */
#include <semaphore.h>      /* sem_open(), sem_destroy(), sem_wait().. */
#include <fcntl.h>          /* O_CREAT, O_EXEC          */

int CANT_INTENTOS_MAPREDUCE = 2;
t_reduce* reduce_final = NULL;
/*
typedef struct{
	int id;
	char resultado[255];
	t_list* nodos_archivo;//list of t_nodo_archivo
}t_reduce_job;
*/

sem_t sem; /*      synch semaphore         *//*shared */
bool REDUCE_FINAL = false;
t_list* mappers;
t_list* reducers;
t_log* logger;
pthread_mutex_t mutex_log;

int JOB_ID;
int conectar_con_marta();
t_list* recibir_mappers(int fd);

int lanzar_hilos_mappers(int fd);
void crearHiloMapper();
int nuevo_hilo_reducer();
int funcionMapping(t_map* map);
//void reduce_job_free(t_reduce_job* job);
int lanzar_hilo_reduce(int fd, t_reduce* reduce);
int avisar_marta_termino(int socket_marta, int map_o_reduce, int map_reduce_id, int resultado);
int nuevo_hilo_mapper(int* map_id_p);
int procesar_mappers(int cant_mappers);
int procesar_reduces();
int funcionReducing(t_reduce* reduce);


int funcionReducing(t_reduce* reduce){
	int resultado;

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Conectando con nodo %s", nodo_base_to_string(reduce->nodo_base_destino));
	pthread_mutex_unlock(&mutex_log);

	int fd = client_socket(reduce->nodo_base_destino->red.ip, reduce->nodo_base_destino->red.puerto);
	if(fd<0){
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "Error conexion con nodo %d, reduce %d\n", fd, reduce->info->id);
		pthread_mutex_unlock(&mutex_log);
		perror("client_sock");
		//exit(1);
		return -1;
	}
	int rs;
	t_msg* msg;

	//envio esto asi entra en el select del reduce
	msg = argv_message(JOB_REDUCER, 0);
	rs = enviar_mensaje(fd, msg);destroy_message(msg);
	if (rs < 0) {
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "Error Enviar msj a nodo %s, reduce %d\n",
				nodo_base_to_string(reduce->nodo_base_destino),
				reduce->info->id);
		pthread_mutex_unlock(&mutex_log);
		perror("client_sock");
		//exit(1);
		return -1;
	}


	rs = enviar_mensaje_reduce(fd, reduce);
	if(rs<0){
		printf("error RRRRR sendmsg2 sock %d, red %d\n", fd, reduce->info->id);
		perror("envisar_msg");
		//exit(1);
		return -1;
	}
	rs = enviar_mensaje_script(fd, JOB_SCRIPT_REDUCER());
	if(rs<0){
		printf("error RRRRR sendmsg3 sock %d, red %d\n", fd, reduce->info->id);
		perror("envisar_msg");
		//exit(1);
		return -1;
	}

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Esperando respuesta a reducer %d en nodo %s",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
	pthread_mutex_unlock(&mutex_log);


	msg = recibir_mensaje(fd);

	if(msg==NULL){
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "msg return NULL, reducer %d en nodo %s",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
		pthread_mutex_unlock(&mutex_log);
		destroy_message(msg);
		return -1;
	}


	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Respuesta recibida de reducer %d en nodo %s",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
	if(msg->header.id==REDUCER_TERMINO){
		log_trace(logger, "El reducer %d en nodo %s termino OK", reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
		resultado = 0;
	}
	else{
		log_trace(logger, "El reducer %d en nodo %s Termino CON ERRORES",reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino));
		resultado = -1;
	}
	pthread_mutex_unlock(&mutex_log);

	destroy_message(msg);

	close(fd);

	return resultado;
}


int funcionMapping(t_map* map){
	t_msg* msg;
	int rs ;
	int resultado;
	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Conectando con nodo %d - %s:%d", map->archivo_nodo_bloque->base->id,  map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	pthread_mutex_unlock(&mutex_log);

	int fd = client_socket(map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	if(fd<0){
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "eRRRORR client_socket %d, map %d\n", fd, map->info->id);
		pthread_mutex_unlock(&mutex_log);
		//perror("client_sock");
		//exit(1);
		return -1;
	}

	printf("____________nuevo Sock %d map:%d\n", fd, map->info->id);
	//envio esto para que entre en el select del MAPPER y espera que recibir un mensaje_map
	msg = argv_message(JOB_MAPPER, 0);
	rs = enviar_mensaje(fd, msg);
	destroy_message(msg);
	if(rs<0){
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "eRRRORR enviar_mensaje JOB_MAPPER %d, map %d\n", fd, map->info->id);
		pthread_mutex_unlock(&mutex_log);
		perror("enviar_mensaje JOB_MAPPER");
		//exit(1);
		return -1;
	}


	rs = enviar_mensaje_map(fd, map);
	if(rs<0){
		printf("RRRORR enviar_msg2 %d, map %d\n", fd, map->info->id);
		//exit(1);
		return -1;
	}

	rs = enviar_mensaje_script(fd, JOB_SCRIPT_MAPPER());
	if(rs<0){
		printf("RRRORR enviar_msg3 %d, map %d\n", fd, map->info->id);
		//exit(1);
		return -1;
	}

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Esperando respuesta sock:%d a mapper %d en nodo %s:%d",fd, map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	pthread_mutex_unlock(&mutex_log);

	msg = recibir_mensaje(fd);

	if(msg==NULL){
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "recv msg NULLLLLLLLLL sock %d, map: %d\n", fd, map->info->id);
		pthread_mutex_unlock(&mutex_log);
		//destroy_message(msg);
		//return -1;
		//creo un msg con id distinto a MAPPER_TERMINO
		msg = argv_message(MAPPER_NOTERMINO, 0);
	}

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Respuesta recibida de mapper %d en nodo %s:%d",map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
	pthread_mutex_unlock(&mutex_log);
	if(msg->header.id==MAPPER_TERMINO){
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "El mapper %d en nodo %s:%d termino OK", map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
		pthread_mutex_unlock(&mutex_log);
		resultado = 0;
	}
	else{
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "El mapper %d en nodo %s:%d Termino CON ERRORES",map->info->id, map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto);
		pthread_mutex_unlock(&mutex_log);
		resultado = -1;
	}
	destroy_message(msg);
	//printf("NINNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN\n");
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

	msg = recibir_mensaje(fd);
	//print_msg(msg);
	int cant_mappers = msg->argv[0];
	log_trace(logger, "Cant. Mappers: %d", cant_mappers);
	destroy_message(msg);
	close(fd);//cierro el fd de marta porque voy a lanzar un connect por cada hilo

	procesar_mappers(cant_mappers);

	procesar_reduces();


	int socket_marta = client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA());

	enviar_mensaje(socket_marta, msg);
	destroy_message(msg);

	//si es distinto de null significa que termino el reduce final OK
	if(reduce_final!=NULL){
		msg = argv_message(JOB_SUBIR_ARCHIVO_FINAL_A_FS,2, JOB_ID, reduce_final->info->id );
		//cuando le envio el mensaje JOB_TERMINO, marta tiene que subir el archivo final generado y decirme si se subio bien o no
		//para saber si realmente termino bien el job, guardando el resultado final en el fs
		msg = recibir_mensaje(fd);
		if (msg->header.id == MARTA_RESULTADO_REDUCE_GUARDADO) {
			pthread_mutex_lock(&mutex_log);
			log_trace(logger, "Resultado final %s guardado en FS OK",
					JOB_RESULTADO());
			pthread_mutex_unlock(&mutex_log);
		} else {
			pthread_mutex_lock(&mutex_log);
			log_trace(logger, "Resultado final %s No se pudo guardar en el FS",
					JOB_RESULTADO());
			pthread_mutex_unlock(&mutex_log);
		}

		reduce_free(reduce_final);
	}


	//cerrar marta
	msg = argv_message(MARTA_SALIR, 0);
	enviar_mensaje(socket_marta, msg);
	destroy_message(msg);
	close(socket_marta);

	return 0;
}

int procesar_reduces(){
	t_msg* msg = NULL;
	pthread_t th_nuevo_reduce;
	//me conecto a marta para que cuando haya un nuevo reduce me avise por este socket
	//t_reduce *reduce;
	void _procesar_reduces() {
		for (;;) {
			sem_wait(&sem);

			if(REDUCE_FINAL){
				//es el reduce final, hago break y me rajo
				pthread_mutex_lock(&mutex_log);
				log_trace(logger, "**************************************************");
				log_trace(logger, "Termino el job final*");
				//log_trace(logger, "Archivo final %s generado en %s",reduce->info->resultado,nodo_base_to_string(reduce->nodo_base_destino));
				pthread_mutex_unlock(&mutex_log);
				break;
			}

			pthread_create(&th_nuevo_reduce, NULL, (void*)nuevo_hilo_reducer, NULL);
			pthread_detach(th_nuevo_reduce);

			//itero hasta que me devuevan el reduce final
		}

		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "Fin proceso reduces");
		pthread_mutex_unlock(&mutex_log);



	}

	pthread_t th_procesar_reduces;
	pthread_create(&th_procesar_reduces, NULL, (void*)_procesar_reduces, NULL);
	pthread_join(th_procesar_reduces, NULL);



	return 0;
}



int nuevo_hilo_reducer(){
	//int reduce_id = *reduce_id_p;

	//connecto con marta
	int socket_marta = client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA());
	t_reduce* reduce = NULL;
	t_msg* msg = NULL;

	//le pido a marta el mapper map_id
	msg = argv_message(REDUCER_GET_ANY, 2, JOB_ID);
	enviar_mensaje(socket_marta, msg);
	destroy_message(msg);

	//recibo el reducer
	reduce = recibir_mensaje_reduce(socket_marta);

	pthread_mutex_lock(&mutex_log);
	log_trace(logger,
			"Reduce_id: %d - nodo destino: %s",
			reduce->info->id, nodo_base_to_string(reduce->nodo_base_destino) );
	pthread_mutex_unlock(&mutex_log);

	int res = 0;
	res = funcionReducing(reduce);

	avisar_marta_termino(socket_marta, REDUCE, reduce->info->id, res);

	bool reducefinal = reduce->final;


	//si el reduce final se hizo bien no limpio el ultimo reduce para usarlo para enviarle a marta el archivo a subir al fs;

	if(reducefinal && res==0){
		reduce_final = reduce;
	}else//sino lo limpio
		reduce_free(reduce);

	if(reducefinal || res<0){//res<0 porque si falla uno cancela tod0, marco como que es final asi sale del for(;;)
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "Termino el reduce FINAL con resultado: %d\n", res);
		REDUCE_FINAL = true;
		sem_post(&sem);
		pthread_mutex_unlock(&mutex_log);
	}


	return res;
}


int procesar_mappers(int cant_mappers){
	pthread_t th_map;
	int *map_id;
	int i;
	for (i = 1; i <= cant_mappers; i++) {
		pthread_mutex_lock(&mutex_log);
		log_trace(logger, "Nuevo Mapper %d", i);
		pthread_mutex_unlock(&mutex_log);

		map_id = malloc(sizeof(*map_id));
		*map_id = i;
		pthread_create(&th_map, NULL, (void*)nuevo_hilo_mapper, (void*)map_id);
		pthread_detach(th_map);
	}
	return 0;
}

int try_map(int map_id){
	int socket_marta;
	t_map* map;
	t_msg* msg;
	int res_map;
	int intentos;

	map = NULL;
	msg = NULL;
	//connecto con marta
	socket_marta = client_socket(JOB_IP_MARTA(), JOB_PUERTO_MARTA());
	//le pido a marta el mapper map_id
	msg = argv_message(MAPPER_GET, 2, JOB_ID, map_id);
	enviar_mensaje(socket_marta, msg);
	destroy_message(msg);

	//recibo el mapper
	map = recibir_mensaje_map(socket_marta);

	print_map(map);

	pthread_mutex_lock(&mutex_log);
	log_trace(logger,
			"Map_id: %d - Ubicacion: id_nodo: %d, %s:%d numero_bloque: %d",
			map->info->id, map->archivo_nodo_bloque->base->id,
			map->archivo_nodo_bloque->base->red.ip,
			map->archivo_nodo_bloque->base->red.puerto,
			map->archivo_nodo_bloque->numero_bloque);
	pthread_mutex_unlock(&mutex_log);

	res_map = funcionMapping(map);
	/*
	intentos =0;
	do {
		res_map = funcionMapping(map);
		intentos++;
	} while (res_map < 0  && intentos<CANT_INTENTOS_MAPREDUCE);
	*/

	//el map_id que tiene aca puede ser distinto porque el
	avisar_marta_termino(socket_marta, MAP, map_id, res_map);

	map_free_all(map);
	return res_map;
}

int nuevo_hilo_mapper(int* map_id_p){
	int map_id = *map_id_p;
	int rs =0;
	do{
		rs = try_map(map_id);
	}while(rs<0);



	FREE_NULL(map_id_p);
	return 0;
}

int avisar_marta_termino(int socket_marta, int map_o_reduce, int map_reduce_id, int resultado){
	pthread_mutex_lock(&mutex_log);
	if(map_o_reduce == MAP)
		log_trace(logger, "Le aviso a Marta que el MAP %d finalizo con resultado: %d", map_reduce_id, resultado);
	else
		log_trace(logger, "Le aviso a Marta que el REDUCE %d finalizo con resultado: %d", map_reduce_id, resultado);
	pthread_mutex_unlock(&mutex_log);

	//le tengo que avisar a marta que termino el map ya sea bien o mal
	//paso el job_id asignado y el resultado y el map_id

	t_msg* msg ;

	if(map_o_reduce == MAP)
		msg = argv_message(MAPPER_TERMINO, 3, JOB_ID, resultado, map_reduce_id);
	else
		msg = argv_message(REDUCER_TERMINO, 3, JOB_ID, resultado, map_reduce_id);

	enviar_mensaje(socket_marta, msg);
	destroy_message(msg);

	pthread_mutex_lock(&mutex_log);
	if(map_o_reduce==MAP)
		log_trace(logger, "Mensaje enviado a marta con resultado: %d del MAP %d", resultado, map_reduce_id);
	else
		log_trace(logger, "Mensaje enviado a marta con resultado: %d del REDUCE %d", resultado, map_reduce_id);
	pthread_mutex_unlock(&mutex_log);

	//si marta me contesta diciendome que hay un nuevo reduce, habilito el sem para que el hilo que lee reducers
	//se desbloquee y lea el reduce
	msg = recibir_mensaje(socket_marta);
	if (msg->header.id == REDUCE_DISPONIBLE) {
		printf("_________________ mapreduce %d genera NUEVOREDUCE\n", map_reduce_id);
		destroy_message(msg);
		//cierro la conexion con marta
		close(socket_marta);

		//habilito nuevo hilo
		sem_post(&sem);
	} else {
		destroy_message(msg);
		//cierro la conexion con marta
		close(socket_marta);
		printf("_________________mapreduce %d no genera ningun MAPREDUCE\n", map_reduce_id);
	}

	return 0;
}


int lanzar_hilo_reduce(int fd, t_reduce* reduce){
	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Comienzo a crear hilo reduce");
	pthread_mutex_unlock(&mutex_log);
	pthread_t th;
	pthread_create(&th, NULL, (void*)funcionReducing, (void*)reduce); //que párametro  ponemos??

	int* res_reduce;
	pthread_join (th, (void**)&res_reduce);

	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Le aviso a Marta que el reduce %d finalizo", reduce->info->id);
	pthread_mutex_unlock(&mutex_log);
	//le tengo que avisar a marta que termino el reduce ya sea bien o mal
	//paso el job_id asignado y el resultado y el map_id
	t_msg* msg = argv_message(JOB_REDUCE_TERMINO, 3, JOB_ID, res_reduce, reduce->info->id);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	pthread_mutex_lock(&mutex_log);
	log_trace(logger, "Mensaje enviado a marta con resultado: %d del reduce %d guardado en %s", res_reduce, reduce->info->id, reduce->info->resultado);
	pthread_mutex_unlock(&mutex_log);
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
