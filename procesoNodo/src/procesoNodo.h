/*
 * procesoNodo.h
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */

#ifndef PROCESONODO_H_
#define PROCESONODO_H_

#include <stdlib.h>         /* exit(), malloc(), free() */
#include <sys/types.h>      /* key_t, sem_t, pid_t      */
#include <sys/shm.h>        /* shmat(), IPC_RMID        */
#include <errno.h>          /* errno, ECHILD            */
#include <semaphore.h>      /* sem_open(), sem_destroy(), sem_wait().. */
#include <fcntl.h>          /* O_CREAT, O_EXEC          */

//#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <commons/string.h>
#include <commons/temporal.h>
#include <commons/txt.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>
#include "config_nodo.h"
#include <nodo.h>
#include <semaphore.h>
#include <math.h>
#include <commons/collections/list.h>

#include <util.h>
//#include "mapreduce.h"
//#include "socket.h"

#include <sys/types.h>
#include <sys/wait.h>

//char FILE_CONFIG[1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/config.txt";
//char FILE_LOG[1024] = "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/log.txt";
char FILE_CONFIG[1024] = "config.txt";
char FILE_LOG[1024] = "log_nodo.txt";

//char CWD[PATH_MAX_LEN];//para guardar el currentworkingdirectory

typedef struct {
	int socket;
	t_msg* msg;
} t_socket_msg;


/*
 * variables
 */
bool FIN = false;
char* _data = NULL;
t_log* logger = NULL;
pthread_mutex_t mutex;
/*
 * declaraciones
 */
char* data_get(char* filename);
void data_destroy();
char* getBloque(int32_t numero);
void setBloque(int32_t numero, char* bloque);

char* getFileContent(char* filename);
bool nodo_es_local(char* ip, int puerto);
void inicializar();
void finalizar();
void probar_conexion_fs();
void iniciar_server_thread();
int NODO_CANT_BLOQUES();
char* generar_nombre_reduce_tmp(t_mapreduce* mapreduce) ;
int procesar_mensaje(int fd, t_msg* msg);
int aplicar_reduce(t_reduce* reduce, char* script);
char* generar_nombre_map_tmp(t_mapreduce* mapreduce);
char* generar_nombre_script();
void iniciar_server();
void agregar_cwd(char* file);

//void* atenderProceso_fork(int socket, t_msg* msg);
//sem_t* sem_crear(int* shmid, key_t* shmkey);
int ordenar_y_guardar_en_temp(char* file_desordenado, char* destino);
int ordenar_map(char* origen, char* destino, pthread_mutex_t* mutex);;
int aplicar_reduce_local_red(t_list* files_reduces, char*script_reduce, char* filename_result);
int aplicar_reduce_ok(t_list* files_reduces, char*script_reduce,	char* filename_result, pthread_mutex_t* mutex) ;

int aplicar_reduce_local(t_list* files, char*script_reduce,
		char* filename_result);
char* convertir_a_temp_path_filename(char* filename);
int thread_aplicar_map(int fd);
int aplicar_map_final(int n_bloque, char* script_map, char* filename_result);
int aplicar_map_ok(int n_bloque, char* script_map, char* filename_result, pthread_mutex_t* mutex);
void incicar_server_sin_select();
bool requiere_hilo(t_msg* msg);
void iniciar_server_fork();
int _aplicar_map(void* param);
void* atenderProceso(t_socket_msg* msg);
//char* ejecutar_script_sort(char* filename);
/*
 * graba el temp concatenandole el timenow en el filename para que sea unico
 */
int grabar_en_temp(char* filename, char* data);
bool file_reduce_es_local(t_files_reduce* fr);
bool file_reduce_es_de_red(t_files_reduce* fr);

bool alguna_key_distinta_null(char** keys, int cant);
int recibir_linea_fd(int fd, char*linea);
int get_index_menor(char** keys, int cant, char* key_menor);
int read_line(char* buff, int fd);
/*
 * devuelve el archivo creado en el  temp del nodo
 */
int aplicar_map(int n_bloque, char* script_map, char* filename_result);


int TAMANIO_DATA=0;;
int CANT_BLOQUES=0;
//int CONTADOR_FTOK=0;

#endif /* PROCESONODO_H_ */
