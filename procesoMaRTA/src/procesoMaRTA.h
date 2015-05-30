#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <libgen.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <util.h>
#include <commons/temporal.h>
#include "config_MaRTA.h"
#include "marta.h"

//char FILE_CONFIG[1024]="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt";
char FILE_LOG[1024] ="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoMaRTA/log.txt";

void iniciar_thread_server_MaRTA();
void iniciar_servidor();
void procesar (int fd, t_msg*msg);
int archivo_a_usar();


t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();
int probar_conexion_filesystem();
t_job* crear_job(int fd);
void verificar_fs_operativo();
int planificar_mappers(int job_id, t_list* bloques_de_datos);
int obtener_numero_copia_disponible_para_map(t_list* nodos_bloque);
t_nodo_estado* marta_buscar_nodo(int id);
int marta_contar_nodo(int id);
t_nodo_estado* marta_create_nodo();
int marta_job_cant_mappers(t_list* archivos);
char* generar_nombre_map(t_job* job);
//t_list* planificar_mappers_con_combiner(t_list* bloques);
//t_list* planificar_mappers_sin_combiner(t_list* bloques);

bool FS_OPERATIVO = false;
t_log* logger;

void iniciar_thread_server_MaRTA() {
	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */
	pthread_t th_procesar;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
	pthread_create(&th_procesar, &tattr, (void*) iniciar_servidor, NULL);
	pthread_attr_destroy(&tattr);
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);
}

void iniciar_servidor () {
	printf("iniciando server marta en el puerto %d\n", MaRTA_PUERTO() );
	//queda bloqueado hasta que le llegue algun mensaje
	server_socket_select(MaRTA_PUERTO(),	(void*) procesar);
}

void verificar_fs_operativo(){
	int fd = client_socket(MaRTA_IP_FS(), MaRTA_PUERTO_FS());
	//
	if(fd<0){
		return ;
	}
	t_msg* msg;
	msg = argv_message(MARTA_HOLA, 0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	msg = recibir_mensaje(fd);
	if(msg->header.id == FS_ESTA_OPERATIVO){
		FS_OPERATIVO =msg->argv[0];
	}
	destroy_message(msg);
}

void procesar (int fd, t_msg*msg){
	int res = 0;
	//int i;

	print_msg(msg);

	switch (msg->header.id) {
		case JOB_HOLA://cuando llega un job nuevo
			//la idea es que le pida al job todos los datos basicos que quiere,
			destroy_message(msg);

			JOB_ID++;//sumo uno en el contador y se lo paso al job
			msg = string_message(MARTA_JOB_ID, "hola job soy marta te paso el id", 1,JOB_ID);
			res = enviar_mensaje(fd, msg);
			destroy_message(msg);
			if(res==-1){
				printf("no se pudo responder el mensaje al job\n");
				break;
			}

			//verifico si el fs esta operativo
			verificar_fs_operativo();

			if(!FS_OPERATIVO){
				printf("FS no operativo\n");
				//le avis al job que no esta operativo
				msg = argv_message(FS_ESTA_OPERATIVO, 1, FS_OPERATIVO);
				enviar_mensaje(fd, msg);
				destroy_message(msg);

				//salgo
				break;////////////////////////
			}

			//le pido al job los archivos que quiere procesar (asumo que ya estan almacenados en el fs)
			//primero me envia la cantidad de archivos

			log_trace(logger, "Creando");
			t_job* job = crear_job(fd);
			log_trace(logger, "Job creado OK");

			//empiezo a planificar mappers
			void _planificar_mappers(t_archivo* archivo){
				log_trace(logger, "Empezando planificacion archivo %s", archivo->nombre);
				//aca agrega los mappers a la lista marta.nodos con prop empezado = false
				planificar_mappers(job->id, archivo->bloque_de_datos);
				log_trace(logger, "Terminado planificacion archivo %s", archivo->nombre);
			}
			list_iterate(job->archivos, (void*)_planificar_mappers);

			//hasta aca tengo creada la planificacion de los mappers
			//ahora tengo que pasarle los marta.nodos.where(!empezado) al job para que empiece a lanzar los hilos mappers
			msg = argv_message(JOB_CANT_MAPPERS,1, marta_job_cant_mappers(job->archivos));
			enviar_mensaje(fd, msg);
			log_trace(logger, "Le paso al job la cantidad de mappers que son(suma de todos los archivos): %d", msg->argv[0]);
			destroy_message(msg);
			log_trace(logger, "Comienzo a enviar los mappers que tiene lanzar el job");

			void _enviar_map_a_job(t_nodo_estado* ne){
				//envio el nombre del archivo
				char* nombre_temp_map = generar_nombre_map(job);
				msg = string_message(JOB_MAPPER, nombre_temp_map, 0);
				enviar_mensaje(fd, msg);
				destroy_message(msg);
				free(nombre_temp_map);

				//2 args, puerto, numero_bloque
				//msg = string_message(JOB_MAPPER, ne->nodo->ip, 2, ne->nodo->puerto, ne->nodo->numero_bloque);
				msg = string_message(JOB_MAPPER, ne->nodo->base->red.ip, 2, ne->nodo->base->red.puerto, ne->nodo->numero_bloque);
				enviar_mensaje(fd, msg);
				destroy_message(msg);


				ne->aplicando_map = true;
				ne->empezo = true;
				log_trace(logger, "Mapper enviado al job %d", job->id);
			}
			list_iterate(marta.nodos, (void*)_enviar_map_a_job);

			//agrego el job a la lista de jobs
			list_add(marta.jobs, (void*) job);





			//primero hay que aplicar el map
			//planifico la mejor forma dependiendo si es combiner o no combiner
			//(de las tres copias del archivo tengo que elegir una sola)
			//una vez que tengo los nodos y bloques optimos
			//le contesto al job diciendole cuales son los nodos y bloques que tiene que usar para el map

			//aca terminaria el primer contacto con el job,
			//el job tendria que empezar a lanzar los hilos reducers
			//y a medida que van terminando me avisa mandandome el mensaje JOB_MAP_TERMINO


			//hay que guardar el job con los mappers que tiene que hacer
			//porque el reduce se empieza cuando terminaron todos los mappers



			break;
		case JOB_MAP_TERMINO:
			//cuando un map termino el job le avisa al fs
			//marcaria el map del job como termino=true

			//luego tengo que verificar si ya terminaron todos los map que tiene que hacer el job
			//si ya terminaron todos los mappers que tenia el job, empiezo a planificar los reduce
			//dependiendo si es combiner o no tengo que planificar distinto

			//en definitiva tengo que pasarle al job los nodos(puede que sean todos el mismo nodo o distintos)
			//y los archivos (el resultado de los maps) a reducir
			//una vez que le envio los nodos y archivos a reducir
			//otra vez espero a que me conteste el job (los hilos reduce) con el mensaje JOB_REDUCE_TERMINO

			break;
		case JOB_REDUCE_TERMINO:
			//a medida que van terminando los reduce los voy marcando en la lista de reducers del job termino=true

			//tengo que verificar si terminaron todos los reduces del job

			//si terminaron todos los reducesd del job hago el reduce final(creo que depende mas de la planificacion)
			//que seria aplicar el reduce final
			//tendria que pedir un reduce pasandole los nodos y los archivos(resultados del reduce)
			//y decirle que guarde el archivo en el job.resultado

			//si todo sale bien le mando el mensaje al job JOB_TERMINO para decirle que termino
			//borro al job de la lista de jobs y el TP esta aprobado!

			break;

		default:
			break;
	}
}

char* generar_nombre_map(t_job* job){
	char* file_map1 = string_new();
	string_append(&file_map1, "job_");
	char str[2];
	sprintf(str, "%d", job->id);
	string_append(&file_map1, str);
	string_append(&file_map1, "_");
	string_append(&file_map1, "map_");
	char* timenow = temporal_get_string_time();
	string_append(&file_map1, timenow);
	free(timenow);
	string_append(&file_map1, ".txt");
	return file_map1;
}

int marta_job_cant_mappers(t_list* archivos){
	int i=0;

	void _contar(t_archivo* archivo){
		i = i + list_size(archivo->bloque_de_datos);
	}
	list_iterate(archivos, (void*)_contar);

	return i;
}

t_archivo* crear_archivo(char* archivo_nombre){
	int i,j, cant_bloques;
	t_archivo* archivo ;
	t_msg* msg;

	//creo el archivo con el nombre
	archivo = marta_create_archivo(archivo_nombre);

	//me conecto con el fs
	int fd = client_socket(MaRTA_IP_FS(), MaRTA_PUERTO_FS());

	//le pido al fs que me devuelva donde esta el archivos
	msg = string_message(MARTA_ARCHIVO_GET_NODOBLOQUE, archivo_nombre, 0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);
	//me pasa primero la cantida de bloques
	msg = recibir_mensaje(fd);
	cant_bloques = msg->argv[0];		//es la cantidad de bloques del archivo
	destroy_message(msg);
	t_archivo_bloque_con_copias* bloque_datos;//para agregar a la lista de blopques del arachivo
	t_archivo_nodo_bloque* anb;//aca una de las copias
	printf("recibiendo lista de bloques del archivo %s - cant_bloques: %d\n", archivo_nombre, cant_bloques);
	for (i = 0; i < cant_bloques; i++) {
		bloque_datos = bloque_de_datos_crear();
		//primero me paso el nro_bloque del archivo
		msg = recibir_mensaje(fd);
		//print_msg(msg);
		bloque_datos->numero_bloque = msg->argv[0];
		//printf("bloque nro: %d\n", bloque_datos->numero_bloque);
		log_trace(logger, "Recibido bloque-nro: %d", bloque_datos->numero_bloque);
		destroy_message(msg);

		for(j=0;j<BLOQUE_CANT_COPIAS;j++){
			msg = recibir_mensaje(fd);
			//me pasa en este orden ip:puerto y el numero_bloque en el nodo y tambien el id_nodo

			anb = marta_create_nodo_bloque(msg->stream, msg->argv[0], msg->argv[1], msg->argv[2]);//ip:port:nro_bloque:nodo_id
			log_trace(logger, "Recibido info bloque %d, %s:%d, nro_bloque(en el nodo): %d, nodo_id", bloque_datos->numero_bloque, anb->base->red.ip, anb->base->red.puerto, anb->numero_bloque, anb->base->id);
			//agrego el bloque(ip-puerto-nro_bloque) a la lista de bloques del archivo
			list_add(bloque_datos->nodosbloque, anb);

			destroy_message(msg);
		}

		log_trace(logger, "Terminado recepcion de copias del bloque %d", bloque_datos->numero_bloque);
		list_add(archivo->bloque_de_datos, (void*)bloque_datos);

	}

	return archivo;
}

/*
 * creo el job y le pido al fs donde estan lso archivos (ip_puerto_nroBlque)
 */
t_job* crear_job(int fd){
	int cant_archivos, i;
	t_msg* msg;
	msg = recibir_mensaje(fd);//el job me manda el archivo de resultado y si soporta combiner
	//creo el job
	t_job* job = NULL;
	log_trace(logger, "archivo resultado: %s, combiner: %d, cant_archivos: %d", msg->stream, msg->argv[0], msg->argv[1]);
	job = marta_create_job(msg->stream, msg->argv[0]);//el archivo resultado y si soporta combiner
	cant_archivos = msg->argv[1];
	destroy_message(msg);
	for (i = 0; i < cant_archivos; i++) {
		//el job me manda los archivos a procesar y busco en que nodos y bloque estan
		char* archivo_nombre;
		//recibo el nombre del archivo
		msg = recibir_mensaje(fd);

		archivo_nombre = string_new();string_append(&archivo_nombre, msg->stream);
		destroy_message(msg);
		log_trace(logger, "Creando t_archivo: %s", archivo_nombre);
		t_archivo* archivo = crear_archivo(archivo_nombre);
		log_trace(logger, "Terminado t_archivo: %s", archivo_nombre);
		free(archivo_nombre);archivo_nombre = NULL;
		//agrego el archivo a procesar

		list_add(job->archivos, archivo);

	}


	return job;
}


t_nodo_estado* marta_buscar_nodo(int id){
	bool _buscar_nodo(t_nodo_estado* ne){
		return ne->nodo->base->id == id;
	}
	return list_find(marta.nodos, (void*)_buscar_nodo);
}

int marta_contar_nodo(int id){
	bool _contar(t_nodo_estado* ne){
		return ne->nodo->base->id==id;
	}
	return list_count_satisfying(marta.nodos, (void*)_contar);
}

/*
 * primero intento obtener el de la c3, sino la c2, en ultima instancia la c1
 * en caso de que todas se esten usando devuelvo el numero de copia con menos cantidad de veces que este usando marta
 */
int  obtener_numero_copia_disponible_para_map(t_list* nodos_bloque){//lista de t_archivo_nodo_bloque
	int n_copia;
	t_archivo_nodo_bloque* anb;

	int MAX = 9999;
	//si no esta vivo busco alguna otra copia
	int copia_cant_veces_usada[BLOQUE_CANT_COPIAS];	//inicializo en un nmero maximo para sacar el menor
	for (n_copia = 1; n_copia <= BLOQUE_CANT_COPIAS; n_copia++) {//o tambien list_size(nodos_Bloque)
		anb = list_get(nodos_bloque, BLOQUE_CANT_COPIAS-n_copia);//tomo el 3, despues 2 y luego 1
		if (nodo_esta_vivo(anb->base->red.ip, anb->base->red.puerto)) {
			copia_cant_veces_usada[n_copia-1] = marta_contar_nodo(anb->base->id);
			log_trace(logger, "Nodo id:%d, %s:%d Disponible - cant-vces-usado-marta(actual): %d", anb->base->id, anb->base->red.ip, anb->base->red.puerto, copia_cant_veces_usada[n_copia-1]);
			if(copia_cant_veces_usada[n_copia-1]==0){//si es igual a 0 no se esta usando, se termino la busqueda de un nodo no usado
				log_trace(logger, "nodo_id %d correspondiente a la copia %d", anb->base->id, n_copia);
				return n_copia;//
			}
		} else{
			log_trace(logger, "Nodo id:%d, %s:%d no disponible", anb->base->id, anb->base->red.ip, anb->base->red.puerto);
			//en caso de que no este vivo le asigno un nro alto asi no lo tiene encuenta cuando saca el menor
			copia_cant_veces_usada[n_copia-1] = MAX;
		}
	}

	log_trace(logger, "todos los nodos estan siendo usados por marta, elijo el que menos veces este siendo usado");
	//si llego hasta aca significa que todos los nodos de las 3 copias estan siendo usados
	//ahora solo me queda sacar el menor y usarlo
	int copia_menos_usada = MAX;
	for (n_copia = 1; n_copia <= BLOQUE_CANT_COPIAS; n_copia++) {
		log_trace(logger, "Copia %d - cant-veces que esta siendo usado actualmente: %d", n_copia, copia_cant_veces_usada[n_copia-1]);
		if (copia_cant_veces_usada[n_copia-1] < copia_menos_usada) {
			copia_menos_usada = n_copia;
		}
	}
	log_trace(logger, "Comienzo a elegir a la copia con el nodo menos usado");
	//me fijo si pudo seleccionar alguno
	if (copia_menos_usada != MAX) {
		//paso el nodo
		anb = list_get(nodos_bloque, copia_menos_usada);//
		log_trace(logger, "La copia menos usada es %d con nodo_id:%d", copia_menos_usada, anb->base->id);
		return copia_menos_usada;
	} else {
		//errror todos los bloques
		return -1;
	}
}


int planificar_mappers(int job_id, t_list* bloques_de_datos){
	t_list* nodos = list_create();
	//int nodo_id;
	int numero_copia;
	int i;
	t_archivo_nodo_bloque* cnb;
	t_archivo_bloque_con_copias* bloque;
	for (i = 0; i < list_size(bloques_de_datos); i++) {
		bloque = list_get(bloques_de_datos, i);
		numero_copia = obtener_numero_copia_disponible_para_map(bloque->nodosbloque);
		cnb =  list_get(bloque->nodosbloque, numero_copia);
		if (cnb!=NULL) {
			log_trace(logger, "obtener_numero_copia_disponible_para_map: %d en %s:%d", cnb->base->id, cnb->base->red.ip, cnb->base->red.puerto);
			t_nodo_estado* ne = marta_create_nodo_estado(cnb);
			//agrego el nodo a la lista de nodos
			list_add(nodos, ne);
		} else {
			log_trace(logger, "ninguna copia disponible para el bloque %d", bloque->numero_bloque);
			return -1;
		}
	}
	//hasta aca tengo la lista de nodos, ahora solo me queda mandarselos al job

	//agrego los nodos a la lsita de nodos de marta
	list_add_all(marta.nodos, nodos);

	return 0;


}

int rutina_con_combiner(){
	return 0;
}
int rutina_sin_combiner(){
	return 0;
}

void enviar_rutina_reduce(){

}

bool soporta_combiner(){
	return false;
}

int archivo_a_usar(){
	return 0;
}

t_list* nodos_usados(){
	return NULL;
}

int hilos_de_mapper(){
	return 0;
}
