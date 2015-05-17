#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "config_MaRTA.h"

void iniciar_thread_server_MaRTA();
void iniciar_servidor();
void procesar (int socket, t_msg*msg);
int archivo_a_usar();
t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();

typedef struct{
	char* ip;//para conectarme con el nodo
	int puerto;//para conectarme con el nodo
	int n_bloque;//para saber que bloque tengo que aplicarle el map
	char* resultado;//el nombre del archivo ya mapeado(solo el nombre porque siempre lo va buscar en el tmp del nodo)
	bool termino;//para saber si termino
}t_map;

typedef struct{
	char* ip;//para conectarme con el nodo
	int puerto;//para conectarme con el nodo
	char* resultado;//el archivo del resultado de reduce(se guarda en el tmp del nodo)
	bool termino;
}t_reduce;

typedef struct {
	t_list* mappers;
	t_list* reducers;
}t_job;

typedef struct {
	t_list* jobs;//guarda estructuras t_job
	char* resultado;//el nombre del archivo resultado final
}t_MaRTA;

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

void procesar (int socket, t_msg*msg){
	int res = 0;
	print_msg(msg);

	switch (msg->header.id) {
		case JOB_HOLA://cuando llega un job nuevo
			//la idea es que le pida al job todos los datos basicos que quiere,


			destroy_message(msg);

			printf("enviandole respuesta al JOB\n");
			msg = string_message(MARTA_HOLA, "hola job soy marta", 0);
			res = enviar_mensaje(socket, msg);
			destroy_message(msg);
			if(res==-1){
				printf("no se pudo responder el mensaje al job\n");
				break;
			}

			//le pido al job los archivos que quiere procesar (asumo que ya estan almacenados en el fs)

			//busco en el fs los nodos y bloques de los archivos

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
