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

e_comando getComando(char* input_user);

void formatear();
void directorio_crear(char* comando);
void nuevosNodos();

t_log* log;
t_config* config;

int main(void) {
	char comando[COMMAND_MAX_SIZE];

	log = log_create(FILE_LOG, "FileSystem", false, LOG_LEVEL_INFO);
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

	log_destroy(log);
	config_destroy(config);
	printf("bb world!!!!");
	return EXIT_SUCCESS;
}

void nuevosNodos() {
	fd_set master;
	fd_set read_fds;
	int fdNuevoNodo;
	int fdmax;
	int newfd;
	struct sockaddr_in remoteaddr; // dirección del cliente
	int addrlen;
	int i;
	char buf[256]; // buffer para datos del cliente


	int port = config_get_int_value(config, "PUERTO_LISTEN");
	fdNuevoNodo = quieroUnPutoSocketDeEscucha(port);

	if (listen(fdNuevoNodo, 1) != 0) {
		handle_error("listen");
		//return EXIT_FAILURE;
	}

	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	FD_SET(fdNuevoNodo, &master);

	fdmax = fdNuevoNodo; // por ahora el maximo

	log_info(log, "inicio thread eschca de nuevos nodos");
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
					addrlen = sizeof(remoteaddr);
					if ((newfd = accept(fdNuevoNodo,(struct sockaddr *) &remoteaddr, &addrlen)) == -1) {
						perror("accept");
					} else {
						FD_SET(newfd, &master); // añadir al conjunto maestro
						if (newfd > fdmax) { // actualizar el máximo
							fdmax = newfd;
						}
						printf("selectserver: new connection from %s on "
								"socket %d\n", inet_ntoa(remoteaddr.sin_addr),
								newfd);
					}
				} else { // gestionar datos de un cliente ya conectado
					Header unHeader;
					if (recibirHeader(i, &unHeader)>0) {
						printf("type: %d, paylength: %d\n", unHeader.type, unHeader.payloadlength);


						void* buffer = malloc(unHeader.payloadlength);

						if(recibirData(i,unHeader,buffer) > 0){
							printf("msj: %s\n", (char*)buffer);
							char* msjOk = "ok";

							if (mandarMensaje(i,2 , 3,msjOk) > 0) {
								printf("el nodo se agrego al fs");
								//log_info(logOrquestador,"Entro Personaje: %c",*simboloRecibido);
							}
						}

					}//fin recHeader
					else{
						close(i); // ¡Hasta luego!
						FD_CLR(i, &master); // eliminar del conjunto maestro
					}


					/*
					 if ((nbytes = recv(i, buf, sizeof(buf), 0)) <= 0) {
					 // error o conexión cerrada por el cliente
					 if (nbytes == 0) { // conexión cerrada
					 printf("selectserver: socket %d hung up\n", i);
					 } else {
					 perror("recv");
					 }
					 close(i); // ¡Hasta luego!
					 FD_CLR(i, &master); // eliminar del conjunto maestro
					 } else {
					 buf[nbytes] = '\0';
					 printf("user: %s\n", buf);
					 char* saludo = malloc(128);
					 saludo = "hola\n";
					 int bytes_enviados = strlen(saludo) + 1;
					 if (sendAll(i, saludo, &bytes_enviados) == -1) {
					 perror("send");
					 }
					 }*/
					/////////////////////////////////////
				} // Esto es ¡TAN FEO!
			}
		}
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

