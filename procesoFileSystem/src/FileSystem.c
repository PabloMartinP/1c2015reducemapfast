/*
 ============================================================================
 Name        : procesoFileSystem.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/string.h>
#include <strings.h>
#include <commons/log.h>
#include <commons/config.h>
#include <unistd.h>
#include <commons/collections/list.h>

#include <socket.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <util.h>
#include <pthread.h>

const int COMMAND_MAX_SIZE = 256;
const char* FILE_DIRECTORIO = "directorio.txt";
const char* FILE_ARCHIVO = "archivo.txt";

#define FILE_CONFIG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/config.txt"
#define FILE_LOG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoFileSystem/log.txt"

typedef enum {
	NODO_AGREGAR,
	NODO_ELIMINAR,
	FORMATEAR,
	NADA,
	SALIR,
	DIRECTORIO_CREAR,
	DIRECTORIO_RENOMBRAR,
	DIRECTORIO_ELIMINAR,
	DIRECTORIO_MOVER,
	DIRECTORIO_LISTAR,
	ARCHIVO_COPIAR_LOCAL_MDFS,
	COPIAR_RENOMBRAR,
	ARCHIVO_ELIMINAR,
	ARCHIVO_MOVER,
	ARCHIVO_LISTAR,
	ARCHIVO_COPIAR_MDFS_LOCAL
} e_comando;

typedef struct {
	int index;
	char nombre[128];
	int padre;
} t_directorio;
typedef struct {
	char nombre[128];
	long int tamanio;
	int directorio;
	bool estado;
//falta la lista de nodos
} t_archivo;

typedef struct {
	int identificador;
	int fd;
} t_nodo;

t_log* logger;
t_config* config;

t_list* nodos;
t_list* nodos_pendientes; //los nodos que estan disponibles pero estan agregados al fs

e_comando getComando(char* input_user);

void formatear();
void directorio_crear(char* comando);
void nuevosNodos();
void procesar_mensaje_nodo(int i, t_msg* msg);
void inicializar();
void fin();
void iniciar_consola();

int main(void) {

	inicializar();

	iniciar_consola();

	return EXIT_SUCCESS;
}

void iniciar_consola() {
	char comando[COMMAND_MAX_SIZE];
	/*
	 * INICIO CONSOLA
	 */
	printf("inicio consola\nIngresar comandos  \n");

	bool fin = false;
	while (!fin) {
		fgets(comando, COMMAND_MAX_SIZE, stdin);

		switch (getComando(comando)) {
		case NODO_AGREGAR:
			printf("comando ingresado: agregar nodo\n");

			//
			break;
		case NODO_ELIMINAR:
			printf("comando ingresado: elimnar nodo\n");

			break;
		case DIRECTORIO_CREAR:
			printf("crear directorio\n");

			directorio_crear(comando);
			break;
		case FORMATEAR:
			formatear();
			break;
		case SALIR:
			printf("comando ingresado: salir\n");
			fin = true;
			break;
		default:
			printf("comando desconocido\n");
			break;
		}

	}
}

void fin() {
	log_destroy(logger);
	config_destroy(config);
	printf("bb world!!!!");

}

void inicializar() {
	logger = log_create(FILE_LOG, "FileSystem", true, LOG_LEVEL_INFO);
	config = config_create(FILE_CONFIG);

	/*
	 * ABRO UN THREAD EL SERVER PARA QUE SE CONECTEN NODOS
	 */

	pthread_t th_nuevosNodos;
	if (pthread_create(&th_nuevosNodos, NULL, (void*) nuevosNodos, NULL) != 0) {
		perror("pthread_create - th_nuevosNodos");
		exit(1);
	}
	//espera a que termine el nodo: bloquea
	//pthread_join(th_nuevosNodos, NULL);
	//pthread_detach(th_nuevosNodos);
}

void nuevosNodos() {
	fd_set master, read_fds;
	int fdNuevoNodo, fdmax, newfd;
	int i;

	fdNuevoNodo = server_socket(config_get_int_value(config, "PUERTO_LISTEN"));

	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	FD_SET(fdNuevoNodo, &master);

	fdmax = fdNuevoNodo; // por ahora el maximo

	log_info(logger, "inicio thread eschca de nuevos nodos");
	// bucle principal
	for (;;) {
		read_fds = master; // cópialo
		if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			exit(1);
		}

		// explorar conexiones existentes en busca de datos que leer
		for (i = 0; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == fdNuevoNodo) {	// gestionar nuevas conexiones
					char * ip;
					newfd = accept_connection_and_get_ip(fdNuevoNodo, &ip);

					printf("nueva conexion desde IP: %s\n", ip);
					FD_SET(newfd, &master); // añadir al conjunto maestro
					if (newfd > fdmax) { // actualizar el máximo
						fdmax = newfd;
					}

				} else { // gestionar datos de un cliente ya conectado
					t_msg *msg = recibir_mensaje(i);
					if (msg == NULL) {
						/* Socket closed connection. */
						//int status = remove_from_lists(i);
						close(i);
						FD_CLR(i, &master);
					} else {
						procesar_mensaje_nodo(i, msg);

					}

				} //fin else procesar mensaje nodo ya conectado
			}
		}
	}

}

void procesar_mensaje_nodo(int i, t_msg* msg) {
	//leer el msg recibido
	print_msg(msg);

	switch (msg->header.id) {
	case NODO_CONECTAR_CON_FS: //primer mensaje del nodo
		destroy_message(msg);
		msg = string_message(FS_NODO_OK, "", 0);
		enviar_mensaje(i, msg);
		break;
	default:
		printf("mensaje desconocido\n");
		break;
	}
}

e_comando getComando(char* input_user) {
	char* comando;

//obtener el nombre del comando que ingreso el user
	comando = string_split(input_user, " ")[0];

	if (string_equals_ignore_case(comando, "agregarnodo\n"))
		return NODO_AGREGAR;
	if (string_equals_ignore_case(comando, "eliminarnodo\n"))
		return NODO_ELIMINAR;
	if (string_equals_ignore_case(comando, "mkdir\n"))
		return DIRECTORIO_CREAR;
	if (string_equals_ignore_case(comando, "formatear\n"))
		return FORMATEAR;
	if (string_equals_ignore_case(comando, "salir\n"))
		return SALIR;

	return NADA;
}

void directorio_crear(char* comando) {
	FILE* file = fopen(FILE_DIRECTORIO, "a+");

	t_directorio dir;
	dir.index = 1; //dir_ultimoIndex();
	strcpy(dir.nombre, "un directorio");
	dir.padre = 0;

//fwrite(&dir, sizeof(t_directorio), 1, file);
	fprintf(file, "hayque grabar el directorio");

	fclose(file);

	printf("grabo el dir\n");
}

void formatear() {
	FILE* file1 = fopen(FILE_DIRECTORIO, "w+");
	FILE* file2 = fopen(FILE_ARCHIVO, "w+");
	fclose(file1);
	fclose(file2);
	printf("se formateo\n");
}

