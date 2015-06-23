/*
 * util.h
 *
 *  Created on: 26/4/2015
 *      Author: utnso
 */

#ifndef UTIL_H_
#define UTIL_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <time.h>
#include <stdlib.h>
 #include <sys/time.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>

#include "commons/collections/list.h"
#include "commons/string.h"

#include <stdbool.h>

#define handle_error(msj) \
	do{perror(msj);exit(EXIT_FAILURE);} while(0)

#define MB_EN_B  1024*1024//1mb
#define LEN_KEYVALUE 4096 //longitud de la key de map o reduce

#define PATH_MAX_LEN 1024 //size maximo de un path

#define REG_SIZE 10
#define BACK_LOG 100

/* Funciones Macro */
#define FREE_NULL(p) \
    {free(p); \
    p = NULL;}


//#define free_null(p) ({free(p);(p)=(NULL);})
/*
 * Compara dos numeros y retorna el mínimo.
 */
#define min(n, m) (n < m ? n : m)

/*
 * Compara dos numeros y retorna el máximo.
 */
#define max(n, m) (n > m ? n : m)

/* FIN Funciones Macro */



/****************** IDS DE MENSAJES. ******************/

typedef enum {

	NODO_CONECTAR_CON_FS, //verifica que el nodo se conecte con el fs
	FS_NODO_OK, //el fs le contesta que ya esta conectado
	NODO_SALIR, //el nodo avisa que se desconecta
	FS_NODO_QUIEN_SOS, //el fs le pregunta al nodo que le diga su ip y puerto
	RTA_FS_NODO_QUIEN_SOS, //devuelve la ip y el port del nodo al fs
	FS_HOLA,
	NODO_HOLA,
	FS_GRABAR_BLOQUE,
	NODO_CHAU,
	NODO_GET_BLOQUE,
	NODO_GET_FILECONTENT_DATA,//envia el mensaje sin header, puro flujo de bytes
	JOB_HOLA,
	MARTA_HOLA,
	JOB_MAP_TERMINO,
	JOB_REDUCE_TERMINO,
	JOB_TERMINO, //indice que el job termino
	MARTA_SALIR, //sale del select marta
	NODO_GET_FILECONTENT,
	FS_AGREGO_NODO,
	FS_ESTA_OPERATIVO,               //para saber si el FS esta operativo o no
	JOB_ARCHIVO,              //le paso los archivos a marta para que me de devuelva los nodos y bloques donde aplicar el mapreduce
	MARTA_JOB_ID,               //marta le pasa el id asignado al job para que se comunique con ese id
	JOB_INFO,         //paso si es combiner o no, el archivo destino del resultado yla cantidad de archivos a procesar
	MARTA_ARCHIVO_GET_NODOBLOQUE,   //marta pide que le den donde esta guardado el archivo
	JOB_CANT_MAPPERS,  //le devuelvo la cantidad de mappers necesarios para la lista de archivos a procesar(sumo ambas)
	JOB_MAPPER,
	JOB_REDUCER,
	REDUCER_TERMINO,
	MAPPER_TERMINO,  //cuando el mapper termino aviso
	MARTA_REDUCE_CANT,
	REDUCE_INFO,
	FIN_REDUCES,
	ANB_NUMERO_BLOQUE,
	ARCHIVO_A_REDUCIR,
	MAPREDUCE_INFO,
	MARTA_REDUCE_NODO,
	MARTA_REDUCE_RESULTADO,
	NODO_BASE,
	JOB_SCRIPT,
	NODO_OK
} t_msg_id;

/****************** ESTRUCTURAS DE DATOS. ******************/


typedef struct{
	char ip[15];
	int puerto;
	char archivo[255];//nombre del archivo guardado en tmp
}t_files_reduce;

typedef struct{
	char ip[15];
	int puerto;
}t_red;

typedef struct {
	int id;
	t_red red;
}t_nodo_base;

typedef struct{
	int job_id;
	int id;
	//t_nodo_base* nodo_base;
	char* resultado;//el nombre del archivo ya mapeado(solo el nombre porque siempre lo va buscar en el tmp del nodo)
	bool empezo;
	bool termino;//para saber si termino
}t_mapreduce;

typedef struct{
	t_nodo_base* nodo_base;
	char archivo[255];//list of string
}t_nodo_archivo;
typedef struct{
	t_mapreduce* info;
	t_nodo_base* nodo_base_destino;
	t_list* nodos_archivo;//list of t_nodo_archivo
	bool final;
}t_reduce;

typedef struct{
	t_nodo_base* base;
	int numero_bloque;//del nodo
}t_archivo_nodo_bloque; //

typedef struct{
	t_mapreduce* info;
	t_archivo_nodo_bloque* archivo_nodo_bloque;

	//int numero_bloque;//para saber a que bloque del archivo tengo que aplicarle el map
}t_map;


typedef struct {
	int8_t type;
	int16_t payloadlength;
}__attribute__ ((__packed__)) t_header_base;

typedef struct {
	t_msg_id id;
	uint32_t length;
	uint16_t argc;
}__attribute__ ((__packed__)) t_header;

typedef struct {
	t_header header;
	char *stream;
	int32_t *argv;
}__attribute__ ((__packed__)) t_msg;

bool file_exists(const char* filename);
//void free_null(void** data);
char* file_combine(char* f1, char* f2);
size_t file_get_size(char* filename);
void* file_get_mapped(char* filename);
void file_mmap_free(char* mapped, char* filename);
float  bytes_to_kilobytes(size_t bytes);
int enviar_mensaje_nodo_base(int fd, t_nodo_base* nb);
t_msg* nodo_base_message(t_nodo_base* nb);
t_nodo_base* recibir_mensaje_nodo_base(int fd);
int enviar_mensaje_reduce(int fd, t_reduce* reduce);
t_nodo_archivo* nodo_archivo_create();
t_reduce* recibir_mensaje_reduce(int fd);
t_nodo_base* nodo_base_new(int id, char* ip, int puerto);
float bytes_to_megabytes(size_t bytes);
int enviar_nodo_base(int fd, t_nodo_base* nb);
t_archivo_nodo_bloque* archivo_nodo_bloque_new(char* ip, int puerto, int numero_bloque, int id);
int enviar_mensaje_map(int fd, t_map* map);
t_archivo_nodo_bloque* archivo_nodo_bloque_create(t_nodo_base* nb, int numero_bloque);
int enviar_mensaje_archivo_nodo_bloque(int fd, t_archivo_nodo_bloque* anb);
int cant_registros(char** registros) ;
int enviar_mensaje_flujo(int unSocket, int8_t tipo, int tamanio, void *buffer);
int recibir_mensaje_flujo(int unSocket, void** buffer);
t_map* recibir_mensaje_map(int fd);
int recibir_mensaje_script_y_guardar(int fd, char* path_destino);
int enviar_mensaje_script(int fd, char* path_script);
void print_map(t_map* map);
int enviar_mensaje_mapreduce(int fd, t_mapreduce* mr);
t_map* map_create(int id, int job_id, char* resultado);
t_mapreduce* mapreduce_create(int id, int job_id, char* resultado);
int enviar_mensaje_sin_header(int sock_fd, int tamanio, void* buffer);
void map_free(t_map* map);



/****************** FUNCIONES SOCKET. ******************/
int server_socket_select(uint16_t port, void (*procesar_mensaje)(int, t_msg*));
/*
 * Crea, vincula y escucha un socket desde un puerto determinado.
 */
int server_socket(uint16_t port);
char* recibir_linea(int sock_fd);

//char* ip_get();

/*
 * Crea y conecta a una ip:puerto determinado.
 */
int client_socket(char* ip, uint16_t port);

/*
 * Acepta la conexion de un socket.
 */
int accept_connection(int sock_fd);
int accept_connection_and_get_ip(int sock_fd, char **ip);
int len_hasta_enter(char* strings);
/*
 * Recibe un t_msg a partir de un socket determinado.
 */
t_msg *recibir_mensaje(int sock_fd);

/*
 * Envia los contenidos de un t_msg a un socket determinado.
 */
int enviar_mensaje(int sock_fd, t_msg *msg);

int enviar_mensaje_nodo_close(int fd);
int recibir_mensaje_nodo_ok(int fd);
int enviar_mensaje_nodo_ok(int fd);

/****************** FUNCIONES T_MSG. ******************/

/*
 * Crea un t_msg sin argumentos, a partir del id.
 */
t_msg *id_message(t_msg_id id);

/*
 * Crea un t_msg a partir de count argumentos.
 */
t_msg *argv_message(t_msg_id id, uint16_t count, ...);

/*
 * Crea un t_msg a partir de un string y count argumentos.
 */
t_msg *string_message(t_msg_id id, char *message, uint16_t count, ...);

/*
 * Libera los contenidos de un t_msg.
 */
void destroy_message(t_msg *mgs);

/****************** FUNCIONES FILE SYSTEM. ******************/

/*
 * Crea un archivo de size bytes de tamaño.
 */
void create_file(char *path, size_t size);

/*
 * Vacía el archivo indicado por path. Si no existe lo crea.
 */
void clean_file(char *path);

/*
 * Lee un archivo y retorna los primeros size bytes de su contenido.
 */
char* read_file(char *path, size_t size);

/*
 * Si existe, copia el contenido del archivo path en dest.
 */
void memcpy_from_file(char *dest, char *path, size_t size);

/*
 * Elimina los primeros size bytes del archivo path, y los retorna.
 */
char* read_file_and_clean(char *path, size_t size);

/*
 * Lee un archivo y retorna todo su contenido.
 */
char* read_whole_file(char *path);

/*
 * Lee un archivo y retorna todo su contenido, vaciándolo.
 */
char* read_whole_file_and_clean(char *path);

/*
 * Abre el archivo indicado por path (si no existe lo crea) y escribe size bytes de data.
 */
void write_file(char *path, char* data, size_t size);

/****************** FUNCIONES AUXILIARES. ******************/

/*
 * Genera una nueva secuencia de enteros pseudo-random a retornar por rand().
 */
void seedgen(void);

/*
 * Muestra los contenidos y argumentos de un t_msg.
 */
void print_msg(t_msg *msg);

/*
 * Convierte t_msg_id a string.
 */
char *id_string(t_msg_id id);
//int convertir_path_absoluto(char** destino, char* file);
char* convertir_path_absoluto(char* file);
t_reduce* reduce_create(int id, int job_id, char* resultado, t_nodo_base* nodo_destino);
char* nodo_base_to_string(t_nodo_base* nb);
void free_split(char** splitted);
int split_count(char** splitted);
int recv_timeout(int s , int timeout);

#endif /* UTIL_H_ */
