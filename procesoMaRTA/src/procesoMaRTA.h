#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <libgen.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <util.h>

#include "config_MaRTA.h"
#include "marta.h"

void iniciar_thread_server_MaRTA();
void iniciar_servidor();
void procesar (int fd, t_msg*msg);
int archivo_a_usar();

bool bloque_alguna_copia_viva(t_bloque_de_datos* cnb);
t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();
int probar_conexion_filesystem();
t_job* crear_job(int fd);
void verificar_fs_operativo();
int planificar_mappers(t_list* bloque_de_datos);
int obtener_nodo_id_disponible_para_map(t_list* nodos_bloque);
t_nodo_estado* marta_buscar_nodo(int id);
int marta_contar_nodo(int id);
t_nodo_estado* marta_create_nodo();
//t_list* planificar_mappers_con_combiner(t_list* bloques);
//t_list* planificar_mappers_sin_combiner(t_list* bloques);

bool FS_OPERATIVO = false;

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
	int res = 0, i;


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

			t_job* job = crear_job(fd);





			//agrego el job a la lista de jobs
			list_add(marta.jobs, (void*) job);

			//hasta aca tengo creada la planificacion de los mappers
			//ahora tengo que pasarle los marta.nodos.where(!empezado) al job para que empiece a lanzar los hilos mappers


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
	t_bloque_de_datos* bloque_datos;
	t_conexion_nodo_bloque* cnb;
	printf("recibiendo lista de bloques del archivo %s - cant_bloques: %d\n", archivo_nombre, cant_bloques);
	for (i = 0; i < cant_bloques; i++) {
		bloque_datos = bloque_de_datos_crear();
		//primero me paso el nro_bloque del archivo
		msg = recibir_mensaje(fd);
		print_msg(msg);
		bloque_datos->n_bloque = msg->argv[0];
		printf("bloque nro: %d\n", bloque_datos->n_bloque);
		destroy_message(msg);

		for(j=0;j<BLOQUE_CANT_COPIAS;j++){
			msg = recibir_mensaje(fd);
			print_msg(msg);
			//me pasa en este orden ip:puerto y el numero_bloque en el nodo y tambien el id_nodo

			cnb = marta_create_nodo_bloque(msg->stream, msg->argv[0], msg->argv[1], msg->argv[2]);//ip:port:nro_bloque:nodo_id
			//agrego el bloque(ip-puerto-nro_bloque) a la lista de bloques del archivo
			list_add(bloque_datos->nodosbloque, cnb);

			destroy_message(msg);
		}

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
		t_archivo* archivo = crear_archivo(archivo_nombre);

		//agrego el archivo a procesar
		list_add(job->archivos, archivo);

		planificar_mappers(archivo->bloque_de_datos);



	}


	return job;
}


t_nodo_estado* marta_buscar_nodo(int id){
	bool _buscar_nodo(t_nodo_estado* ne){
		return ne->id == id;
	}
	return list_find(marta.nodos, (void*)_buscar_nodo);
}

int marta_contar_nodo(int id){
	bool _contar(t_nodo_estado* ne){
		return ne->id==id;
	}
	return list_count_satisfying(marta.nodos, (void*)_contar);
}

/*
 * primero intento obtener el de la c3, sino la c2, en ultima instancia la c1
 * en caso de que todas se esten usando devuelvo el nodo que tenga menos uso
 */
int  obtener_nodo_id_disponible_para_map(t_list* nodos_bloque){
	int i;
	t_conexion_nodo_bloque* cnb;

	int MAX = 9999;
	//si no esta vivo busco alguna otra copia
	int cant_c[BLOQUE_CANT_COPIAS];	//inicializo en un nmero maximo para sacar el menor
	for (i = 1; i <= BLOQUE_CANT_COPIAS; i++) {//o tambien list_size(nodos_Bloque)
		cnb = list_get(nodos_bloque, BLOQUE_CANT_COPIAS-i);//tomo el 3, despues 2 y luego 1
		if (bloque_alguna_copia_viva(cnb)) {
			cant_c[i-1] = marta_contar_nodo(cnb->id);
			if(cant_c[i-1]==0){//si es igual a 0 no se esta usando, se termino la busqueda de un nodo no usado
				return i;//id nodo dispnible
			}
		} else
			//en caso de que no este vivo le asigno un nro alto asi no lo tiene encuenta cuando saca el menor
			cant_c[i-1] = MAX;
	}
	//si llego hasta aca significa que todos los nodos de las 3 copias estan siendo usados
	//ahora solo me queda sacar el menor y usarlo
	int nodo_menos_usado = MAX;
	for (i = 1; i <= BLOQUE_CANT_COPIAS; i++) {
		if (cant_c[i-1] < nodo_menos_usado) {
			nodo_menos_usado = i;
		}
	}
	//me fijo si pudo seleccionar alguno
	if (nodo_menos_usado != MAX) {
		//paso el nodo
		return nodo_menos_usado;
	} else {
		//errror todos los bloques
		return -1;
	}
}

bool bloque_alguna_copia_viva(t_bloque_de_datos* bloque_datos){
	//si no existe, lo agrego tranquilamente porque no esta haciendo nada el nodo
	//primero verifico si esta vivo
/*
	int cnb;
	int fd = client_socket(cnb->ip, cnb->puerto);
	if (fd < 0) {
		printf("nodo no conectado id:%d - %s:%d", cnb->id, cnb->ip,
				cnb->puerto);
		return false;
	}
	t_msg* msg = argv_message(NODO_HOLA, 0);
	if (enviar_mensaje(fd, msg) < 0) {
		close(fd);
		destroy_message(msg);
		return false;
	}

	close(fd);
	destroy_message(msg);
*/
	return true;
}

int planificar_mappers(t_list* bloque_de_datos){
	t_list* nodos = list_create();
	int nodo_id;
	//verifico si hay algun nodo vivo al menos
	if(list_any_satisfy(bloque_de_datos, (void*)bloque_alguna_copia_viva)){
		int i;
		t_bloque_de_datos* bloque;
		for(i=0;i<list_size(bloque_de_datos);i++){
			bloque = list_get(bloque_de_datos, i);

			nodo_id= obtener_nodo_id_disponible_para_map(bloque->nodosbloque);
			if(nodo_id>0){
				t_nodo_estado* ne = marta_create_nodo_estado();
				ne->id = nodo_id;
				//agrego el nodo a la lista de nodos
				list_add(nodos, ne);
			}else{
				return -1;
			}
		}
		//hasta aca tengo la lista de nodos, ahora solo me queda mandarselos al job

		//agrego los nodos a la lsita de nodos de marta
		list_add_all(marta.nodos, nodos);

		return 0;
		/*
		void _crear_map(t_conexion_nodo_bloque* cnb){
			t_nodo_estado* ne = marta_buscar_nodo(cnb->id);
			if(ne==NULL){
				//si es null el nodo es nuevo, no esta haciendo nada
				ne = marta_create_nodo_estado();
				ne->aplicando_map=true;
				return;
			}
		}
		list_iterate(bloques, (void*)_crear_map);
		*/
	}else{
		//algun bloque tiene sus tres copias desconectadas
		return -1;
	}

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
