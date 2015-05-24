/*
 * util.c
 *
 *  Created on: 26/4/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "util.h"
#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>

size_t file_get_size(char* filename) {
	struct stat st;
	stat(filename, &st);
	return st.st_size;
}

//une alos dos string con una barra
char* file_combine(char* f1, char* f2) {
	char* p = NULL;
	p = string_new();

	string_append(&p, f1);
	string_append(&p, "/");
	string_append(&p, f2);

	return p;

}
/*
//char ip[15];

char* ip_get(){
	int fd;
	struct ifreq ifr;

	fd = socket(AF_INET, SOCK_DGRAM, 0);

	ifr.ifr_addr.sa_family = AF_INET;

	snprintf(ifr.ifr_name, IFNAMSIZ, "eth0");

	ioctl(fd, SIOCGIFADDR, &ifr);



	strcpy(ip, inet_ntoa(((struct sockaddr_in *) &ifr.ifr_addr)->sin_addr));
	//fprintf(ip, "%s", inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr));

	printf("%s", ip);

	close(fd);
	return ip;
}
*/

void file_mmap_free(char* mapped, char* filename) {
	munmap(mapped, file_get_size(filename));
}

int cant_registros(char** registros) {
	int i = 0;
	while (registros[i] != NULL) {
		i++;
	}
	return i;
}


/*
 * devuelve el arhivo mappeado modo lectura y escritura
 */
void* file_get_mapped(char* filename) {
	//el archivo ya esta creado con el size maximo
	void* mapped = NULL;
	struct stat st;
	int fd = 0;
	fd = open(filename, O_RDWR);
	if (fd == -1) {
		handle_error("open");
	}

	stat(filename, &st);
	//printf("%ld\n", st.st_size);
	int size = st.st_size;

	mapped = mmap(NULL, size, PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	if (mapped == MAP_FAILED) {
		handle_error("mmap");
	}

	return mapped;
}

/*
void free_null(void** data) {
	free(*data);
	*data = NULL;
	data = NULL;
}*/

bool file_exists(const char* filename) {
	bool rs = true;

	FILE* f = NULL;
	f = fopen(filename, "r");
	if (f != NULL) {
		fclose(f);
		rs = true;
	} else
		rs = false;

	return rs;
}

/*
 ***************************************************
 */
int server_socket_select(uint16_t port, void (*procesar_mensaje)(int, t_msg*)) {
	fd_set master, read_fds;
	int fdNuevoNodo, fdmax, newfd;
	int i;

	if ((fdNuevoNodo = server_socket(port)) < 0) {
		handle_error("No se pudo iniciar el server");
	}
	printf("server iniciado en %d\n", port);

	FD_ZERO(&master); // borra los conjuntos maestro y temporal
	FD_ZERO(&read_fds);
	FD_SET(fdNuevoNodo, &master);

	fdmax = fdNuevoNodo; // por ahora el maximo

	//log_info(logger, "inicio thread eschca de nuevos nodos");
	// bucle principal
	for (;;) {
		read_fds = master; // cópialo
		if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {
			handle_error("Error en select");
		}

		// explorar conexiones existentes en busca de datos que leer
		for (i = 0; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // ¡¡tenemos datos!!
				if (i == fdNuevoNodo) {	// gestionar nuevas conexiones
					//char * ip;
					//newfd = accept_connection_and_get_ip(fdNuevoNodo, &ip);
					newfd = accept_connection(fdNuevoNodo);
					//printf("nueva conexion desde IP: %s\n", ip);
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
						procesar_mensaje(i, msg);

					}

				} //fin else procesar mensaje nodo ya conectado
			}
		}
	}
	return 0;
}
int server_socket(uint16_t port) {
	int sock_fd, optval = 1;
	struct sockaddr_in servername;

	/* Create the socket. */
	sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (sock_fd < 0) {
		perror("socket");
		return -1;
	}

	/* Set socket options. */
	if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval)
			== -1) {
		perror("setsockopt");
		return -2;
	}

	/* Fill ip / port info. */
	servername.sin_family = AF_INET;
	servername.sin_addr.s_addr = htonl(INADDR_ANY);
	servername.sin_port = htons(port);

	/* Give the socket a name. */
	if (bind(sock_fd, (struct sockaddr *) &servername, sizeof servername) < 0) {
		perror("bind");
		return -3;
	}

	/* Listen to incoming connections. */
	if (listen(sock_fd, 1) < 0) {
		perror("listen");
		return -4;
	}

	return sock_fd;
}

int client_socket(char *ip, uint16_t port) {
	int sock_fd;
	struct sockaddr_in servername;

	/* Create the socket. */
	sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (sock_fd < 0) {
		perror("socket");
		return -1;
	}

	/* Fill server ip / port info. */
	servername.sin_family = AF_INET;
	servername.sin_addr.s_addr = inet_addr(ip);
	servername.sin_port = htons(port);
	memset(&(servername.sin_zero), 0, 8);

	/* Connect to the server. */
	if (connect(sock_fd, (struct sockaddr *) &servername, sizeof servername)
			< 0) {
		perror("connect");
		return -2;
	}

	return sock_fd;
}

int accept_connection(int sock_fd) {
	struct sockaddr_in clientname;
	size_t size = sizeof clientname;

	int new_fd = accept(sock_fd, (struct sockaddr *) &clientname,
			(socklen_t *) &size);
	if (new_fd < 0) {
		perror("accept");
		return -1;
	}

	return new_fd;
}

int accept_connection_and_get_ip(int sock_fd, char **ip) {
	struct sockaddr_in clientname;
	size_t size = sizeof clientname;

	int new_fd = accept(sock_fd, (struct sockaddr *) &clientname,
			(socklen_t *) &size);
	if (new_fd < 0) {
		perror("accept");
		return -1;
	}

	*ip = inet_ntoa(clientname.sin_addr);

	return new_fd;
}

t_msg *id_message(t_msg_id id) {

	t_msg *new = malloc(sizeof *new);

	new->header.id = id;
	new->argv = NULL;
	new->stream = NULL;
	new->header.argc = 0;
	new->header.length = 0;

	return new;
}

t_msg *argv_message(t_msg_id id, uint16_t count, ...) {
	va_list arguments;
	va_start(arguments, count);

	int32_t *val = malloc(count * sizeof *val);

	int i;
	for (i = 0; i < count; i++) {
		val[i] = va_arg(arguments, uint32_t);
	}

	t_msg *new = malloc(sizeof *new);
	new->header.id = id;
	new->header.argc = count;
	new->argv = val;
	new->header.length = 0;
	new->stream = NULL;

	va_end(arguments);

	return new;
}

int enviar_mensaje_sin_header(int sock_fd, int tamanio, void* buffer){
	int total=0, pending =tamanio;
	char *bufferAux = malloc(pending);
	memcpy(bufferAux, buffer, tamanio);

	/* Send message(s). */

	while (total < pending) {
		int sent = send(sock_fd, bufferAux, tamanio, MSG_NOSIGNAL);
		if (sent < 0) {
			FREE_NULL(bufferAux);
			return -1;
		}
		total += sent;
		pending -= sent;
	}
	FREE_NULL(bufferAux);
	return 0;
}

//Mande un mensaje a un socket determinado usando una estructura
int enviar_mensaje_flujo(int unSocket, int8_t tipo, int tamanio, void *buffer) {
	t_header_base header;
	int auxInt;
	//Que el tamanio lo mande
	void* bufferAux;

	header.type = tipo;
	header.payloadlength = tamanio;
	bufferAux = malloc(sizeof(t_header_base) + tamanio);
	memcpy(bufferAux, &header, sizeof(t_header_base));
	memcpy((bufferAux + (sizeof(t_header_base))), buffer, tamanio);
	auxInt = send(unSocket, bufferAux, (sizeof(t_header_base) + tamanio), 0);
	FREE_NULL(bufferAux);
	return auxInt;
}

int recibir_mensaje_flujo(int unSocket, void** buffer) {

	t_header_base header;
	int auxInt;
	if ((auxInt = recv(unSocket, &header, sizeof(t_header_base), 0)) >= 0) {
		*buffer = malloc(header.payloadlength);
		if ((auxInt = recv(unSocket, *buffer, header.payloadlength, 0)) >= 0) {
			return auxInt;
		}
	}
	return auxInt;

}

t_msg *string_message(t_msg_id id, char *message, uint16_t count, ...) {
	va_list arguments;
	va_start(arguments, count);

	int32_t *val = NULL;
	if (count > 0)
		val = malloc(count * sizeof *val);

	int i;
	for (i = 0; i < count; i++) {
		val[i] = va_arg(arguments, uint32_t);
	}

	t_msg *new = malloc(sizeof *new);
	new->header.id = id;
	new->header.argc = count;
	new->argv = val;
	new->header.length = strlen(message);
	new->stream = string_duplicate(message);

	va_end(arguments);

	return new;
}

char* recibir_linea(int sock_fd){
	char* linea = malloc(LEN_KEYVALUE);
	char caracter=NULL;
	int bytes_leidos = 0;
	int status;
	do{
		status = recv(sock_fd, &caracter, 1, 0);
		linea[bytes_leidos] = caracter;
		if(caracter == '\n'){
			status = -2;//fin de linea
		}
		if(caracter =='\0')
			status = -3;
		bytes_leidos++;
	}while(status>0);

	if(status==-2){//si es igual a menos dos
		linea[bytes_leidos] = '\0';
		return linea;
	}
	else
	{
		if(status==-3){//termino de leer el archivo
			FREE_NULL(linea);
			return NULL;
		}
		else{
			FREE_NULL(linea);
			perror("El nodo perdio conexion\n");
			return NULL;
		}

	}
}

t_msg *recibir_mensaje(int sock_fd) {
	t_msg *msg = malloc(sizeof *msg);
	msg->argv = NULL;
	msg->stream = NULL;

	/* Get message info. */
	int status = recv(sock_fd, &(msg->header), sizeof(t_header), MSG_WAITALL);
	if (status <= 0) {
		/* An error has ocurred or remote connection has been closed. */
		FREE_NULL(msg);
		return NULL;
	}

	/* Get message data. */
	if (msg->header.argc > 0) {
		msg->argv = malloc(msg->header.argc * sizeof(uint32_t));

		if (recv(sock_fd, msg->argv, msg->header.argc * sizeof(uint32_t),
		MSG_WAITALL) <= 0) {
			FREE_NULL(msg->argv);
			FREE_NULL(msg);
			return NULL;
		}
	}

	if (msg->header.length > 0) {
		msg->stream = malloc(msg->header.length + 1);

		if (recv(sock_fd, msg->stream, msg->header.length, MSG_WAITALL) <= 0) {
			FREE_NULL(msg->stream);
			FREE_NULL(msg->argv);
			FREE_NULL(msg);
			return NULL;
		}

		msg->stream[msg->header.length] = '\0';
	}

	return msg;
}

int enviar_mensaje(int sock_fd, t_msg *msg) {
	int total = 0;
	int pending = msg->header.length + sizeof(t_header)
			+ msg->header.argc * sizeof(uint32_t);
	char *buffer = malloc(pending);

	/* Fill buffer with the struct's data. */
	memcpy(buffer, &(msg->header), sizeof(t_header));

	int i;
	for (i = 0; i < msg->header.argc; i++)
		memcpy(buffer + sizeof(t_header) + i * sizeof(uint32_t), msg->argv + i,
				sizeof(uint32_t));

	memcpy(buffer + sizeof(t_header) + msg->header.argc * sizeof(uint32_t),
			msg->stream, msg->header.length);

	/* Send message(s). */
	while (total < pending) {
		int sent = send(sock_fd, buffer,
				msg->header.length + sizeof msg->header
						+ msg->header.argc * sizeof(uint32_t), MSG_NOSIGNAL);
		if (sent < 0) {
			FREE_NULL(buffer);
			return -1;
		}
		total += sent;
		pending -= sent;
	}

	FREE_NULL(buffer);

	return total;
}

void destroy_message(t_msg *msg) {
	if (msg->header.length  ){
		FREE_NULL(msg->stream);
	}
	else
		if(msg->stream != NULL && string_is_empty(msg->stream))
			FREE_NULL(msg->stream);
	if (msg->header.argc && msg->argv != NULL)
		FREE_NULL(msg->argv);
	FREE_NULL(msg);
}

void create_file(char *path, size_t size) {

	FILE *f = fopen(path, "wb");

	fseek(f, size - 1, SEEK_SET);

	fputc('\n', f);

	fclose(f);
}

void clean_file(char *path) {

	FILE *f = fopen(path, "wb");
	//perror("fopen");

	fclose(f);
}

char* read_file(char *path, size_t size) {

	FILE *f = fopen(path, "rb");
	if (f == NULL) {
		perror("fopen");
		exit(EXIT_FAILURE);
	}

	char *buffer = malloc(size + 1);
	if (buffer == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}

	fread(buffer, size, 1, f);

	fclose(f);

	buffer[size] = '\0';

	return buffer;
}

void memcpy_from_file(char *dest, char *path, size_t size) {

	FILE *f = fopen(path, "rb");

	if (f != NULL) {
		fread(dest, size, 1, f);
		fclose(f);
	}
}

char *read_file_and_clean(char *path, size_t size) {

	FILE *f = fopen(path, "rb");
	if (f == NULL) {
		perror("fopen");
		exit(EXIT_FAILURE);
	}

	char *buffer = malloc(size + 1);
	if (buffer == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}

	fread(buffer, size, 1, f);

	fclose(f);

	f = fopen(path, "wb");

	fclose(f);

	buffer[size] = '\0';

	return buffer;
}

char *read_whole_file(char *path) {

	FILE *f = fopen(path, "rb");
	if (f == NULL) {
		perror("fopen");
		exit(EXIT_FAILURE);
	}

	fseek(f, 0, SEEK_END);
	long fsize = ftell(f);
	fseek(f, 0, SEEK_SET);

	char *buffer = malloc(fsize + 1);
	if (buffer == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}

	fread(buffer, fsize, 1, f);

	fclose(f);

	buffer[fsize] = '\0';

	return buffer;
}

char *read_whole_file_and_clean(char *path) {

	FILE *f = fopen(path, "rb");
	if (f == NULL) {
		perror("fopen");
		exit(EXIT_FAILURE);
	}

	fseek(f, 0, SEEK_END);
	long fsize = ftell(f);
	fseek(f, 0, SEEK_SET);

	char *buffer = malloc(fsize + 1);
	if (buffer == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}

	fread(buffer, fsize, 1, f);

	fclose(f);

	buffer[fsize] = '\0';

	return buffer;
}

void write_file(char *path, char *data, size_t size) {

	FILE *f = fopen(path, "wb");

	fwrite(data, 1, size, f);

	fclose(f);
}

void print_msg(t_msg *msg) {
	int i;
	puts("\n==================================================");
	printf("CONTENIDOS DEL MENSAJE:\n");
	char* id = id_string(msg->header.id);
	printf("- ID: %s\n", id);
	FREE_NULL(id);

	for (i = 0; i < msg->header.argc; i++) {
		;
		printf("- ARGUMENTO %d: %d\n", i + 1, msg->argv[i]);
	}

	printf("- TAMAÑO: %d\n", msg->header.length);
	printf("- CUERPO: ");

	for (i = 0; i < msg->header.length; i++)
		putchar(*(msg->stream + i));
	puts("\n==================================================\n");
}

char *id_string(t_msg_id id) {
	char *buf;
	switch (id) {
	case NODO_GET_BLOQUE:
		buf = strdup("NODO_GET_BLOQUE");
		break;
	case NODO_CONECTAR_CON_FS:
		buf = strdup("NODO_CONECTAR_CON_FS");
		break;
	case FS_NODO_OK:
		buf = strdup("FS_NODO_OK");
		break;
	case NODO_SALIR:
		buf = strdup("NODO_SALIR");
		break;
	case FS_NODO_QUIEN_SOS:
		buf = strdup("FS_NODO_QUIEN_SOS");
		break;
	case RTA_FS_NODO_QUIEN_SOS:
		buf = strdup("RTA_FS_NODO_QUIEN_SOS");
		break;
	case FS_HOLA:
		buf = strdup("FS_HOLA");
		break;
	case NODO_HOLA:
		buf = strdup("NODO_HOLA");
		break;
	case FS_GRABAR_BLOQUE:
		buf = strdup("FS_GRABAR_BLOQUE");
		break;
	case NODO_CHAU:
		buf = strdup("NODO_CHAU");
		break;
	default:
		buf = string_from_format("%d, <AGREGAR A LA LISTA>", id);
		break;
	case JOB_HOLA:
		buf = strdup("JOB_HOLA");
		break;
	case MARTA_HOLA:
		buf = strdup("MARTA_HOLA");
		break;
	case NODO_GET_FILECONTENT:
		buf = strdup("NODO_GET_FILECONTENT");
		break;


	}
	return buf;
}


/*
 *
 */

float bytes_to_kilobytes(size_t bytes){
	return bytes / (1024 + 0.0);
}
float bytes_to_megabytes(size_t bytes){
	return bytes / ((1024*1024) + 0.0);
}
/*
int convertir_path_absoluto(char**destino, char* file){
	//char* destino = malloc(PATH_MAX_LEN);
	if (getcwd(*destino, PATH_MAX_LEN) == NULL)
		handle_error("getcwd() error");
	strcat(*destino, file);

	return 0;
}
*/
char* convertir_path_absoluto(char* file){
	char* destino = malloc(PATH_MAX_LEN);
	if (getcwd(destino, PATH_MAX_LEN) == NULL)
		handle_error("getcwd() error");
	strcat(destino, file);
	return destino;
}


