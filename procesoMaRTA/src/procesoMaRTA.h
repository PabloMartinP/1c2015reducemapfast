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

#include <nodo.h>


bool FIN = false;

//char FILE_CONFIG[1024]="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt";
char FILE_LOG[1024] ="/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoMaRTA/log.txt";

void iniciar_thread_server_MaRTA();
void iniciar_servidor();
void procesar (int fd, t_msg*msg);
int archivo_a_usar();

int enviar_maps(int fd, t_job* job);
t_reduce* crear_reduce_final(t_job* job, t_nodo_base* nb);
t_reduce* crear_reduce_local(t_job* job, t_nodo_base* nb);

t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();
int probar_conexion_filesystem();
t_job* crear_job(int fd);
int marta_cant_mappers_no_empezados();
void verificar_fs_operativo();
//int planificar_mappers(int job_id, t_list* bloques_de_datos);
int planificar_mappers(t_job* job, t_list* bloques_de_datos);
int obtener_numero_copia_disponible_para_map(t_list* nodos_bloque, t_job* job);
//t_nodo_estado_map* marta_buscar_nodo(int id);
int marta_contar_nodo(int id, t_job* job);
int job_cantidad_mappers_por_nodo_id(t_job* job, int id_nodo);
void marta_destroy();
void marta_map_destroy(int job_id, int map_id);
void map_destroy(t_map* map);
//t_nodo_estado_map* marta_create_nodo();
void marta_job_destroy(int id);
t_archivo_job* crear_archivo(char* archivo_nombre);
void job_destroy(t_job* job);
int marta_job_cant_mappers(t_list* archivos);
int enviar_reduce_local(int fd, t_nodo_base* nb, t_job* job);
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
	//msg = argv_message(FS_ESTA_OPERATIVO, 0);
	msg = argv_message(FS_ESTA_OPERATIVO, 0);
	enviar_mensaje(fd, msg);
	destroy_message(msg);

	msg = recibir_mensaje(fd);
	if(msg->header.id == FS_ESTA_OPERATIVO){
		FS_OPERATIVO =msg->argv[0];
	}
	destroy_message(msg);
	close(fd);
}

int job_crear_y_planificar_mappers(t_job* job){
	//empiezo a planificar mappers
	void _planificar_mappers(t_archivo_job* archivo) {
		log_trace(logger, "Empezando planificacion archivo %s",	archivo->nombre);
		//aca agrega los mappers a la lista marta.nodos con prop empezado = false
		planificar_mappers(job, archivo->bloque_de_datos);
		log_trace(logger, "Terminado planificacion archivo %s",	archivo->nombre);
	}
	list_iterate(job->archivos, (void*) _planificar_mappers);



	return 0;
}

void procesar (int fd, t_msg*msg){
	int res = 0;
	int job_id, map_id, reduce_id;
	t_job* job;
	t_nodo_base* nb;
	//int i;
	t_reduce* reduce;

	//print_msg(msg);

	switch (msg->header.id) {
		case JOB_HOLA://cuando llega un job nuevo
			//la idea es que le pida al job todos los datos basicos que quiere,
			destroy_message(msg);
			log_trace(logger, "NuevoJob > ********************************************");
			log_trace(logger, "Un nuevo job se conecto a MARTA");
			JOB_ID++;//sumo uno en el contador y se lo paso al job
			msg = argv_message(MARTA_JOB_ID, 1,JOB_ID);
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


			//agrego los mappers a la listad de marta.nodos_map
			job_crear_y_planificar_mappers(job);


			enviar_maps(fd, job);

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
		case MAPPER_TERMINO:

			//cuando un map termino el job le avisa al fs
			job_id = msg->argv[0];
			map_id = msg->argv[2];
			res = msg->argv[1];//resultado
			log_trace(logger, "INICIO MAPPER_TERMINO *****************************************************************");
			log_trace(logger, "TERMINO el MAP %d del job %d. Resultado: %d", map_id, job_id, res);

			destroy_message(msg);

			//no lo destruyo ahora pero despues tengo que destruirlo, por ahora sololo marco como terminado
			//marta_map_destroy(job_id, map_id);

			//marcaria el map del job como termino=true o false
			if(res)
				marta_marcar_map_como_terminado(job_id, map_id);
			else
				marta_marcar_map_como_fallido(job_id, map_id);


			//luego tengo que verificar si ya terminaron todos los map que tiene que hacer el job
			//si ya terminaron todos los mappers que tenia el job, empiezo a planificar los reduce
			//dependiendo si es combiner o no tengo que planificar distinto

			job = job_buscar(job_id);
			if(job->combiner){
				log_trace(logger, "Es con combiner, me fijo si ahora puedo hacer el reduce de los archivos locales al nodo que termino el map");
				//en el caso de combiner, tengo que aplicar el reduce sobre todos los archivos locales al nodo
				//tengo que verificar si ya terminaron todos los mappers locales al nodo del map que recien termino
				t_map* map = map_buscar(job, map_id);
				if(job_obtener_nodo_con_todos_sus_mappers_terminados(job->mappers, map->archivo_nodo_bloque->base)){
					log_trace(logger, "Si puede, el nodo %s tiene que aplicar map sobre sus archivos temporales", nodo_base_to_string(map->archivo_nodo_bloque->base));
					//ya termino todos sus mappers
					//lanzo el reduce de los archivos locales al nodo

					log_trace(logger, "El nodo %s tiene que aplicar map sobre sus archivos temporales", nodo_base_to_string(map->archivo_nodo_bloque->base));
					enviar_reduce_local(fd, map->archivo_nodo_bloque->base, job);
				}else{
					log_trace(logger, "No puede, Todavia no terminaron sus maps locales");
				}

			}else{
				//si es sin combiner tengo que esperar a que terminen todos los mappers
				log_trace(logger, "Es SIN combiner, no hago ningun reduce hasta el final");
			}

			log_trace(logger, "FIN MAPPER_TERMINO *****************************************************************");
			break;
		case JOB_REDUCE_TERMINO:
			log_trace(logger, "INICIO JOB_REDUCE_TERMINO *****************************************************************");
			//print_msg(msg);
			job_id = msg->argv[0];
			reduce_id = msg->argv[2];
			res = msg->argv[1];//resultado
			log_trace(logger, "TERMINO el REDUCE %d del job %d. Resultado: %d", reduce_id, job_id, res);
			destroy_message(msg);

			//marcaria el reduce del job como termino=true o false
			if (res)
				marta_marcar_reduce_como_terminado(job_id, reduce_id);
			else
				marta_marcar_reduce_como_fallido(job_id, reduce_id);

			/////////////////////////////////////////////////////////
			job = NULL;;reduce = NULL;
			job = job_buscar(job_id);

			reduce = reduce_buscar(job, reduce_id);
			if(!reduce->final){
				if (job->combiner) {
					if (job_terminaron_todos_los_map_y_reduce(job)) {
						//hago el reduce final
						log_trace(logger, "El job %d termino todos sus mappers y reducers", job->id);
						log_trace(logger, "*************************************************");
						log_trace(logger, "Hago el reduce final");

						//tengo que seleccionar el nodo que mas archivos tenga archivos locales
						//
						nb = NULL;
						nb = job_obtener_nodo_para_reduce_final_combiner(job);

						log_trace(logger, "Nodo con mas archivos locales %s (donde se aplica el reduce final)\n", nodo_base_to_string(nb));

						t_reduce* reduce = NULL;
						reduce = crear_reduce_final(job, nb);

						log_trace(logger, "Creado reduce %d  para el job %d, cant-nodo-archivo: %d, nodo_destino: %s", reduce->info->id, job->id, list_size(reduce->nodos_archivo), nodo_base_to_string(reduce->nodo_base_destino));
						log_trace(logger, "Resultado: %s", reduce->info->resultado);
						list_add(job->reducers, reduce);

						enviar_mensaje_reduce(fd, reduce);
						/*//envio el mensaje que indica que no hay mas reduces
						msg = argv_message(FIN_REDUCES, 0);
						enviar_mensaje(fd, msg);
						destroy_message(msg);
	*/
						//marco al job como que lanzo el reduce final
						//job->empezo_reduce_final = true;

					} else {
						log_trace(logger, "El job %d NO termino todos sus mappers y reducers", job->id);
					}
				} else {
					log_trace(logger, "Es SIN combiner, me fijo si ya terminaron todos sus maps para hacer el reduce final");
				}
			}


			//tengo que verificar si terminaron todos los reduces del job

			//si terminaron todos los reducesd del job hago el reduce final(creo que depende mas de la planificacion)
			//que seria aplicar el reduce final
			//tendria que pedir un reduce pasandole los nodos y los archivos(resultados del reduce)
			//y decirle que guarde el archivo en el job.resultado

			//si todo sale bien le mando el mensaje al job JOB_TERMINO para decirle que termino
			//borro al job de la lista de jobs y el TP esta aprobado!
			log_trace(logger, "FIN JOB_REDUCE_TERMINO *****************************************************************");
			break;
		case JOB_TERMINO:
			log_trace(logger, "JOB_TERMINO *****************************************************************");
			log_trace(logger, "Termino el job %d", msg->argv[0]);

			marta_job_destroy(msg->argv[0]);

			destroy_message(msg);
			break;

		case MARTA_SALIR:
			FIN = true;
			break;
		default:
			break;
	}
}

t_reduce* crear_reduce_final(t_job* job, t_nodo_base* nb){
	t_reduce* reduce = NULL;

	JOB_REDUCE_ID++;
	char* resultado = generar_nombre_reduce(job->id, JOB_REDUCE_ID);
	reduce = reduce_create(JOB_REDUCE_ID, job->id, resultado, nb);
	//marco como el reduce final
	reduce->final = true;


	FREE_NULL(resultado);

	t_nodo_archivo* na = NULL;

	void _crear_reduce(t_reduce* r) {
		na = NULL;
		//verifico el nodo haya terminado
		if (r->info->termino) {
			na = nodo_archivo_create();

			//copio el nombre para no complicarme con los frees
			strcpy(na->archivo, r->info->resultado);
			na->nodo_base = r->nodo_base_destino;

			//log_trace(logger, "%s", nodo_base_to_string(r->nodo_base_destino));
			//log_trace(logger, "%s", nodo_base_to_string(na->nodo_base));
			//printf("%s\n", na->archivo);
			log_trace(logger, "Archivo %s en nodo %s", na->archivo, nodo_base_to_string(na->nodo_base));
			//log_trace(logger, "Archivo %s en nodo id:%d, %s:%d", na->archivo, na->nodo_base->id, na->nodo_base->red.ip, na->nodo_base->red.puerto);
			//log_trace(logger, "Archivo %s(en %s) reducir en nodo id:%d, %s:%d", na->archivo, na->nodo_base->id, na->nodo_base->red.ip, na->nodo_base->red.puerto);
			//agrego a la lista el archivo
			list_add(reduce->nodos_archivo, (void*) na);
		}
	}
	list_iterate(job->reducers, (void*) _crear_reduce);


	return reduce;
}

int enviar_maps(int fd, t_job* job){
	t_msg* msg;
	//hasta aca tengo creada la planificacion de los mappers
	//ahora tengo que pasarle los marta.nodos.where(!empezado) al job para que empiece a lanzar los hilos mappers
	msg = argv_message(JOB_CANT_MAPPERS, 1, list_size(job->mappers));
	enviar_mensaje(fd, msg);
	log_trace(logger,	"Le paso al job la cantidad de mappers que son(suma de todos los archivos): %d",msg->argv[0]);
	destroy_message(msg);
	log_trace(logger, "Comienzo a enviar los mappers que tiene lanzar el job");

	void _enviar_map_a_job(t_map* map) {
		//envio el map al job


		enviar_mensaje_map(fd, map);

		map->info->empezo = true;
		log_trace(logger,
				"Mapper %d enviado al job %d, Nodo_id: %d, %s:%d, bloque:%d",
				map->info->id, job->id, map->archivo_nodo_bloque->base->id,
				map->archivo_nodo_bloque->base->red.ip, map->archivo_nodo_bloque->base->red.puerto,
				map->archivo_nodo_bloque->numero_bloque);


	}
	list_iterate(job->mappers, (void*) _enviar_map_a_job);


	return 0;
}

t_reduce* crear_reduce_local(t_job* job, t_nodo_base* nb){
	t_reduce* reduce = NULL;

	JOB_REDUCE_ID++;
	char* resultado = generar_nombre_reduce(job->id, JOB_REDUCE_ID);
	reduce = reduce_create(JOB_REDUCE_ID, job->id, resultado, nb);

	FREE_NULL(resultado);

	t_nodo_archivo* na = NULL;

	void _crear_reduce_local(t_map* map){
		na = NULL;
		na = nodo_archivo_create();

		//verifico el nodo haya terminado y sea el  mismo, la idea es qeudarme con todos los locales
		if(map->info->termino && nodo_base_igual_a(*(map->archivo_nodo_bloque->base), *nb)){
			//copio el nombre para no complicarme con los frees
			strcpy(na->archivo, map->info->resultado);
			na->nodo_base = nb;

			log_trace(logger, "Archivo a reducir: %s en nodo_id: %d, %s:%d", na->archivo, na->nodo_base->id, na->nodo_base->red.ip, na->nodo_base->red.puerto);
			//agrego a la lista el archivo
			list_add(reduce->nodos_archivo, (void*)na);
		}
	}
	list_iterate(job->mappers, (void*)_crear_reduce_local);

	return reduce;
}

int enviar_reduce_local(int fd, t_nodo_base* nb, t_job* job){
	log_trace(logger, "Creando reduce local a enviar para el job %d", job->id);

	t_reduce* reduce = NULL;

	reduce = crear_reduce_local(job, nb);

	//hasta aca tengo la variable reduce cargada, ahora tengo que mandarsela al job
	////////////////////////////////////////////////////////////////////////////////
	//log_trace(logger, "Creado reduce %d  para el job %d, cant-nodo-archivo: %d, nodo_destino: %d - %s:%d", reduce->info->id, job->id, list_size(reduce->nodos_archivo), reduce->nodo_base_destino->id, reduce->nodo_base_destino->red.ip, reduce->nodo_base_destino->red.puerto);
	log_trace(logger, "Creado reduce %d  para el job %d, cant-nodo-archivo: %d, nodo_destino: %s", reduce->info->id, job->id, list_size(reduce->nodos_archivo), nodo_base_to_string(reduce->nodo_base_destino));
	log_trace(logger, "Guardar resultado en %s", reduce->info->resultado);

	list_add(job->reducers, reduce);

	log_trace(logger, "Empiezo a enviarle el reduce al job");

	enviar_mensaje_reduce(fd, reduce);

	log_trace(logger, "Fin envio de reduce %d", reduce->info->id);

	return 0;
}





int marta_job_cant_mappers(t_list* archivos){
	int i=0;

	void _contar(t_archivo_job* archivo){
		i = i + list_size(archivo->bloque_de_datos);
	}
	list_iterate(archivos, (void*)_contar);

	return i;
}

t_archivo_job* crear_archivo(char* archivo_nombre){
	int i,j, cant_bloques;
	t_archivo_job* archivo ;
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
		bloque_datos->parte_numero = msg->argv[0];
		//printf("bloque nro: %d\n", bloque_datos->numero_bloque);
		log_trace(logger, "Recibido parte_numero: %d", bloque_datos->parte_numero);
		destroy_message(msg);

		for(j=0;j<BLOQUE_CANT_COPIAS;j++){
			msg = recibir_mensaje(fd);
			//me pasa en este orden ip:puerto y el numero_bloque en el nodo y tambien el id_nodo

			anb = marta_create_nodo_bloque(msg->stream, msg->argv[0], msg->argv[1], msg->argv[2]);//ip:port:nro_bloque:nodo_id
			log_trace(logger, "Recibido info parte_numero %d, %s:%d, nro_bloque(en el nodo): %d, nodo_id", bloque_datos->parte_numero, anb->base->red.ip, anb->base->red.puerto, anb->numero_bloque, anb->base->id);
			//agrego el bloque(ip-puerto-nro_bloque) a la lista de bloques del archivo
			list_add(bloque_datos->nodosbloque, anb);

			destroy_message(msg);
		}

		log_trace(logger, "Terminado recepcion de copias de la parte numero %d", bloque_datos->parte_numero);
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
		t_archivo_job* archivo = crear_archivo(archivo_nombre);
		log_trace(logger, "Terminado t_archivo: %s", archivo_nombre);
		free(archivo_nombre);archivo_nombre = NULL;
		//agrego el archivo a procesar

		list_add(job->archivos, archivo);

	}


	return job;
}

/*
t_nodo_estado_map* marta_buscar_nodo(int id){
	bool _buscar_nodo(t_nodo_estado_map* ne){
		return ne->nodo->base->id == id;
	}
	return list_find(marta.nodos_mappers, (void*)_buscar_nodo);
}
*/
int marta_contar_nodo(int id, t_job* job){
	int cant = 0;
	void _contar_mappers_job(t_job* job_map){
		cant = cant + job_cantidad_mappers_por_nodo_id(job_map, id);
	}
	list_iterate(marta.jobs, (void*)_contar_mappers_job);


	cant = cant + job_cantidad_mappers_por_nodo_id(job, id);

	return cant;
}

int job_cantidad_mappers_por_nodo_id(t_job* job, int id_nodo){
	bool _contar_maps(t_map* map){
		return map->archivo_nodo_bloque->base->id == id_nodo;
	}
	return list_count_satisfying(job->mappers, (void*)_contar_maps);

}


/*
 * primero intento obtener el de la c3, sino la c2, en ultima instancia la c1
 * en caso de que todas se esten usando devuelvo el numero de copia con menos cantidad de veces que este usando marta
 */
int  obtener_numero_copia_disponible_para_map(t_list* nodos_bloque, t_job* job){//lista de t_archivo_nodo_bloque
	int n_copia;
	t_archivo_nodo_bloque* anb;

	int MAX = 9999;
	//si no esta vivo busco alguna otra copia
	int copia_cant_veces_usada[BLOQUE_CANT_COPIAS];	//inicializo en un nmero maximo para sacar el menor
	log_trace(logger, "Cantidad de copias del bloque: %d", BLOQUE_CANT_COPIAS);
	for (n_copia = BLOQUE_CANT_COPIAS-1; n_copia >= 0; n_copia--) {//o tambien list_size(nodos_Bloque)
		anb = list_get(nodos_bloque, n_copia);//tomo el 3, despues 2 y luego 1, por eso empieza en n_copia = BLOQUE_CANT_COPIAS
		log_trace(logger, "Copia numero: %d, nodo_id: %d, %s:%d", n_copia+1, anb->base->id, anb->base->red.ip, anb->base->red.puerto);
		if (nodo_esta_vivo(anb->base->red.ip, anb->base->red.puerto)) {
			copia_cant_veces_usada[n_copia] = marta_contar_nodo(anb->base->id, job);
			log_trace(logger, "Nodo id:%d, %s:%d Disponible - cant-vces-usado-marta(actual): %d", anb->base->id, anb->base->red.ip, anb->base->red.puerto, copia_cant_veces_usada[n_copia]);
			if(copia_cant_veces_usada[n_copia]==0){//si es igual a 0 no se esta usando, se termino la busqueda de un nodo no usado
				log_trace(logger, "nodo_id %d correspondiente a la copia %d", anb->base->id, n_copia+1);
				return n_copia;//-1 porque la lista empieza en -1
			}
		} else{
			log_trace(logger, "Nodo id:%d, %s:%d no disponible", anb->base->id, anb->base->red.ip, anb->base->red.puerto);
			//en caso de que no este vivo le asigno un nro alto asi no lo tiene encuenta cuando saca el menor
			copia_cant_veces_usada[n_copia] = MAX;
		}
	}

	log_trace(logger, "todos los nodos estan siendo usados por marta, elijo el que menos veces este siendo usado");
	//si llego hasta aca significa que todos los nodos de las 3 copias estan siendo usados
	//ahora solo me queda sacar el menor y usarlo
	int copia_menos_usada = -1;t_nodo_base* nb;
	int copia_cant_veces_menos_usada = MAX;
	for (n_copia = 0; n_copia < BLOQUE_CANT_COPIAS; n_copia++) {
		anb = list_get(nodos_bloque, n_copia);
		log_trace(logger, "Copia %d nodo %s - cant-veces que esta siendo usado actualmente: %d", n_copia+1, nodo_base_to_string(anb->base), copia_cant_veces_usada[n_copia]);
		if (copia_cant_veces_usada[n_copia] < copia_cant_veces_menos_usada) {
			copia_cant_veces_menos_usada = copia_cant_veces_usada[n_copia];
			copia_menos_usada = n_copia;
		}
	}
	log_trace(logger, "Comienzo a elegir a la copia con el nodo menos usado");
	//me fijo si pudo seleccionar alguno
	if (copia_menos_usada != MAX) {
		//paso el nodo
		anb = list_get(nodos_bloque, copia_menos_usada);//
		log_trace(logger, "La copia menos usada es %d con nodo_id:%d", copia_menos_usada+1, anb->base->id);
		return copia_menos_usada;
	} else {
		//errror todos los bloques
		log_trace(logger, "TOdos los nodos desconectados!!!!!!!!!!!!!!");
		return -1;
	}
}


int planificar_mappers(t_job* job, t_list* bloques_de_datos){



	//int nodo_id;
	int numero_copia;
	int i;
	t_archivo_nodo_bloque* cnb;
	t_archivo_bloque_con_copias* bloque;
	for (i = 0; i < list_size(bloques_de_datos); i++) {
		bloque = list_get(bloques_de_datos, i);
		log_trace(logger, "Inicio Planificacion parte %d", bloque->parte_numero);


		numero_copia = obtener_numero_copia_disponible_para_map(bloque->nodosbloque, job);
		cnb =  list_get(bloque->nodosbloque, numero_copia);
		if (cnb!=NULL) {
			log_trace(logger, "obtener_numero_copia_disponible_para_map: %d en %s:%d", cnb->base->id, cnb->base->red.ip, cnb->base->red.puerto);
			JOB_MAP_ID++;
			//t_nodo_estado_map* ne = marta_create_nodo_estado(cnb);
			char* nombre_temp_map = generar_nombre_map(job->id, JOB_MAP_ID);
			t_map* map = marta_create_map(JOB_MAP_ID, job->id, nombre_temp_map, cnb);
			FREE_NULL(nombre_temp_map);
			//lo agrego a la lista de marta
			list_add(job->mappers, (void*)map);
		} else {
			log_trace(logger, "Ninguna copia disponible para parte_numero %d", bloque->parte_numero);
			//tengo que borrar los nodos que le agregue a marta, es decir lo que no estan empezados

			return -1;
		}
		log_trace(logger, "Fin Planificacion parte %d", bloque->parte_numero);
	}
	//hasta aca tengo la lista de nodos, ahora solo me queda mandarselos al job



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
