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
t_list* nodos_usados();
int hilos_de_mapper();
bool soporta_combiner();
int rutina_con_combiner();
int rutina_sin_combiner();
void enviar_rutina_reduce();
int probar_conexion_filesystem();



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

void procesar (int fd, t_msg*msg){
	int res = 0, i;
	int cant_archivos;
	char* archivo;

	print_msg(msg);

	switch (msg->header.id) {
		case JOB_HOLA://cuando llega un job nuevo
			//la idea es que le pida al job todos los datos basicos que quiere,
			destroy_message(msg);

			printf("enviandole respuesta al JOB\n");
			JOB_ID++;//sumo uno en el contador y se lo paso al job
			msg = string_message(MARTA_JOB_ID, "hola job soy marta te paso el id", 1,JOB_ID);
			res = enviar_mensaje(fd, msg);
			destroy_message(msg);
			if(res==-1){
				printf("no se pudo responder el mensaje al job\n");
				break;
			}

			//le pido al job los archivos que quiere procesar (asumo que ya estan almacenados en el fs)
			//primero me envia la cantidad de archivos
			//aca me pasa el archivo resultdo, si siporta o no combiner y la cantidad de archivos a proces
			msg = recibir_mensaje(fd);
			//creo el job
			t_job* job = marta_create_job(msg->stream, msg->argv[0]);//el archivo resultado y si soporta combiner
			cant_archivos = msg->argv[1];
			destroy_message(msg);
			for(i=0;i<cant_archivos;i++){
				msg = recibir_mensaje(fd);
				print_msg(msg);
				//agrego el archivo a procesar
				list_add(job->archivos, msg->stream);
				destroy_message(msg);
			}
			//agrego el job a la lista de jobs
			list_add(marta.jobs, (void*) job);

			//busco en el fs los nodos y bloques de los archivos qu eme paso el job


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

int probar_conexion_filesystem(){

	int fd ;
	if((fd = client_socket(MaRTA_IP(), MaRTA_PUERTO()))<0){
		printf("No se pudo conectar con el fs en %s:%d\n", MaRTA_IP(), MaRTA_PUERTO());
		return -1;
	}
	t_msg* msg = string_message(MARTA_HOLA, "", 0);
	if(enviar_mensaje(fd, msg)<0){
		printf("No se pudo enviar el mensaje al fs\n");
		return -2;
	}

	destroy_message(msg);
	msg = recibir_mensaje(fd);

	destroy_message(msg);
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
