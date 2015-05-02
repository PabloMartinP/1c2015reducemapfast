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
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <commons/string.h>


#include <stdbool.h>

#define handle_error(msj) \
	do{perror(msj);exit(EXIT_FAILURE);} while(0)

/*
 *
 */

#define MB_EN_B  1024*1024//1mb
#define REG_SIZE 4

/* Funciones Macro */

/*
 * Compara dos numeros y retorna el mínimo.
 */
#define min(n, m) (n < m ? n : m)

/*
 * Compara dos numeros y retorna el máximo.
 */
#define max(n, m) (n > m ? n : m)

/*
 * Sleep en microsegundos.
 */
#define msleep(usecs) usleep(usecs * 1000)

/*
 * Alternativa sin undefined behavior a fflush(STDIN) para usar con scanf().
 */
#define clean_stdin_buffer() scanf("%*[^\n]")

/*
 * RNG. Retorna valores entre 0 y limit.
 */
#define randomize(limit) (rand() % (limit + 1))

/*
 * Divide dos enteros redondeando hacia arriba.
 */
#define divRoundUp(n,m) (n % m == 0 ? n / m : n / m + 1)

/* FIN Funciones Macro */

/****************** IDS DE MENSAJES. ******************/

typedef enum {

	NODO_CONECTAR_CON_FS,//verifica que el nodo se conecte con el fs
	FS_NODO_OK, //el fs le contesta que ya esta conectado
	NODO_SALIR, //el nodo avisa que se desconecta
	FS_NODO_QUIEN_SOS, //el fs le pregunta al nodo que le diga su ip y puerto
	RTA_FS_NODO_QUIEN_SOS, //devuelve la ip y el port del nodo al fs
	FS_HOLA,
	NODO_HOLA,
	FS_GRABAR_BLOQUE
} t_msg_id;



/****************** ESTRUCTURAS DE DATOS. ******************/

typedef struct {
			int8_t type;
			int16_t payloadlength;
		}  __attribute__ ((__packed__)) t_header_base;



typedef struct {
	t_msg_id id;
	uint32_t length;
	uint16_t argc;
} __attribute__ ((__packed__)) t_header;

typedef struct {
	t_header header;
	char *stream;
	int32_t *argv;
} __attribute__ ((__packed__)) t_msg;



bool file_exists(const char* filename);
void free_null(void* data);
char* file_combine(char* f1, char* f2);
size_t file_get_size(char* filename);
void* file_get_mapped(char* filename);
void file_mmap_free(void* mapped, char* filename);

int enviar_mensaje_flujo(int unSocket, int8_t tipo, int tamanio, void *buffer);
int recibir_mensaje_flujo(int unSocket, void** buffer) ;

/****************** FUNCIONES SOCKET. ******************/
int server_socket_select(uint16_t port, void(*procesar_mensaje)(int,t_msg*));
/*
 * Crea, vincula y escucha un socket desde un puerto determinado.
 */
int server_socket(uint16_t port);

/*
 * Crea y conecta a una ip:puerto determinado.
 */
int client_socket(char* ip, uint16_t port);

/*
 * Acepta la conexion de un socket.
 */
int accept_connection(int sock_fd);
int accept_connection_and_get_ip(int sock_fd, char **ip);

/*
 * Recibe un t_msg a partir de un socket determinado.
 */
t_msg *recibir_mensaje(int sock_fd);

/*
 * Envia los contenidos de un t_msg a un socket determinado.
 */
int enviar_mensaje(int sock_fd, t_msg *msg);


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
 * Agrega nuevos argumentos a un mensaje (estilo FIFO).
 */
t_msg *modify_message(t_msg_id new_id, t_msg *old_msg, uint16_t new_count, ...);

/*
 * Elimina todos los argumentos existentes de un mensaje y agrega nuevos.
 */
t_msg *remake_message(t_msg_id new_id, t_msg *old_msg, uint16_t new_count, ...);


/*
 * Libera los contenidos de un t_msg.
 */
void destroy_message(t_msg *mgs);


/****************** FUNCIONES FILE SYSTEM. ******************/

/*
 * Crea un archivo de size bytes de tamaño.
 */
void create_file(char *path,size_t size);

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
void memcpy_from_file(char *dest,char *path,size_t size);

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
void write_file(char *path,char* data,size_t size);


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


#endif /* UTIL_H_ */