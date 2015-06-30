/*
 * util.c
 *
 *  Created on: 26/4/2015
 *      Author: utnso
 */

#include "util.h"
//Size of each chunk of data received, try changing this
		#define CHUNK_SIZE 512
		/*
		    Receive data in multiple chunks by checking a non-blocking socket
		    Timeout in seconds
		*/
		int recv_timeout(int s , int timeout)
		{
		    int size_recv , total_size= 0;
		    struct timeval begin , now;
		    char chunk[CHUNK_SIZE];
		    double timediff;

		    //make socket non blocking
		    fcntl(s, F_SETFL, O_NONBLOCK);

		    //beginning time
		    gettimeofday(&begin , NULL);

		    while(1)
		    {
		        gettimeofday(&now , NULL);

		        //time elapsed in seconds
		        timediff = (now.tv_sec - begin.tv_sec) + 1e-6 * (now.tv_usec - begin.tv_usec);

		        //if you got some data, then break after timeout
		        if( total_size > 0 && timediff > timeout )
		        {
		            break;
		        }

		        //if you got no data at all, wait a little longer, twice the timeout
		        else if( timediff > timeout*2)
		        {
		            break;
		        }

		        memset(chunk ,0 , CHUNK_SIZE);  //clear the variable
		        if((size_recv =  recv(s , chunk , CHUNK_SIZE , 0) ) < 0)
		        {
		            //if nothing was received then we want to wait a little before trying again, 0.1 seconds
		            usleep(100000);
		        }
		        else
		        {
		            total_size += size_recv;
		            printf("%s" , chunk);
		            //reset beginning time
		            gettimeofday(&begin , NULL);
		        }
		    }

		    return total_size;
		}

size_t file_get_size(char* filename) {
	struct stat st;
	stat(filename, &st);
	return st.st_size;
}



void map_free(t_map* map){
	//FREE_NULL(map->info->nodo_base);
	FREE_NULL(map->info->resultado);
	FREE_NULL(map->info);
	//free(map->archivo_nodo_bloque);para simplificar el free lo hace el archivo_destroy
	FREE_NULL(map);

}

t_mapreduce* mapreduce_create(int id, int job_id, char* resultado){
	t_mapreduce* new = malloc(sizeof*new);
	new->id = id;
	new->resultado = string_new();
	string_append(&(new->resultado), resultado);
	new->empezo = false;
	new->termino = false;
	new->job_id = job_id;
	return new;
}

t_map* map_create(int id, int job_id, char* resultado){
	t_map* new = malloc(sizeof*new);

	new->info = mapreduce_create(id, job_id, resultado);

	return new;
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

int len_hasta_enter(char* strings){
		int i=0;
		while(strings[i]!='\n' && strings[i]!='\0')
			i++;

		return i+1;
	}


void file_mmap_free(char* mapped, char* filename) {
	munmap(mapped, file_get_size(filename));
}

t_archivo_nodo_bloque* archivo_nodo_bloque_new(char* ip, int puerto, int numero_bloque, int id){
	t_archivo_nodo_bloque* new = malloc(sizeof*new);

	new->numero_bloque = numero_bloque;
	new->base = nodo_base_new(id, ip, puerto);

	return new	;
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
	size_t size = st.st_size;

	mapped = mmap(NULL, size, PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	if (mapped == MAP_FAILED) {
		if(size==0)
			printf("el archivo tiene tamaño 0. Imposible mappear");
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
						printf("Conexion cerrada %d\n", i);
						close(i);
						FD_CLR(i, &master);
					} else {
						//print_msg(msg);
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
	//if (listen(sock_fd, BACK_LOG) < 0) {
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
		perror("sockettt");
		return -1;
	}

	//make socket non blocking
	//fcntl(sock_fd, F_SETFL, O_NONBLOCK);

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

	int new_fd = accept(sock_fd, (struct sockaddr *) &clientname,(socklen_t *) &size);
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

/*
 * id NODO_BASE
 */
t_msg* nodo_base_message(t_nodo_base* nb){
	t_msg* msg =NULL;
	msg = string_message(NODO_BASE, nb->red.ip,2, nb->red.puerto, nb->id);
	return msg;
}

int enviar_mensaje_mapreduce(int fd, t_mapreduce* mr){
	t_msg* msg = NULL;
	//envio resultado, id, job_id
	msg = string_message(MAPREDUCE_INFO, mr->resultado, 2, mr->id, mr->job_id);
	int rs = enviar_mensaje(fd, msg);
	destroy_message(msg);
	return rs;
}

t_mapreduce* recibir_mensaje_mapreduce(int fd){
	t_mapreduce* mr = NULL;
	t_msg* msg = NULL;
	//envio resultado, id, job_id
	msg = recibir_mensaje(fd);

	mr = mapreduce_create(msg->argv[0], msg->argv[0], msg->stream);

	destroy_message(msg);
	return mr;
}

int enviar_mensaje_archivo_nodo_bloque(int fd, t_archivo_nodo_bloque* anb){
	// envio la info para conectarse al nodo
	enviar_mensaje_nodo_base(fd, anb->base);

	t_msg* msg = NULL;
	msg = argv_message(ANB_NUMERO_BLOQUE, 1, anb->numero_bloque);
	int rs = enviar_mensaje(fd, msg);
	destroy_message(msg);

	return rs;
}

t_archivo_nodo_bloque* archivo_nodo_bloque_create(t_nodo_base* nb, int numero_bloque){
	t_archivo_nodo_bloque* new = malloc(sizeof*new);

	new->numero_bloque = numero_bloque;
	new->base = nb;
	return new;
}

t_archivo_nodo_bloque* recibir_mensaje_archivo_nodo_bloque(int fd){
	t_nodo_base* nb = NULL;
	int numero_bloque = 0;
	//recibo la info para conectarse al nodo
	nb = recibir_mensaje_nodo_base(fd);

	// recibo el numero de bloque
	t_msg* msg = NULL;
	msg = recibir_mensaje(fd);

	if(msg->header.id == ANB_NUMERO_BLOQUE){
		numero_bloque = msg->argv[0];
	}
	destroy_message(msg);

	t_archivo_nodo_bloque* anb = NULL;
	anb = archivo_nodo_bloque_create(nb, numero_bloque);
	return anb;
}


int enviar_mensaje_map(int fd, t_map* map){
	int rs;
	//envio primero la info de mapreduce
	rs = enviar_mensaje_mapreduce(fd, map->info);
	rs = enviar_mensaje_archivo_nodo_bloque(fd, map->archivo_nodo_bloque);

	return rs;
}

int enviar_mensaje_reduce(int fd, t_reduce* reduce){
	t_msg* msg;
	//envio el nombre del archivo donde almacena el resultado, el reduce_id, el job_id y la cantidad de nodo-archivo que tiene que recibir
	//printf("Enviando el nombre del archivo resultado: %s",reduce->info->resultado);

	//envio el nombre resultado, red_id, job_id, cant_nodos_archivo, final
	msg = string_message(REDUCE_INFO, reduce->info->resultado, 4, reduce->info->id, reduce->info->job_id, list_size(reduce->nodos_archivo), reduce->final);
	enviar_mensaje(fd, msg);
	destroy_message(msg);

	//ahora le paso el nodo_destino (ip:puerto, id_nodo: %d)
	enviar_mensaje_nodo_base(fd, reduce->nodo_base_destino);

	//printf("Enviando lista de t_nodo_archivo");
	void _enviar_reduce_a_job(t_nodo_archivo* na) {
		//envio el nombre del archivo en tmp a aplicar el reduce
		//printf("Enviando Nombre_tmp a reducir: %s", na->archivo);
		msg = string_message(ARCHIVO_A_REDUCIR, na->archivo, 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		//envio info del nodo para conectarse
		//ip, puerto, id_nodo
		//printf("Enviando nodo-id: %d - %s:%d", na->nodo_base->id, na->nodo_base->red.ip, 2, na->nodo_base->red.puerto);
		//printf("Enviando info nodo %s", nodo_base_to_string(na->nodo_base));
		enviar_mensaje_nodo_base(fd, na->nodo_base);
		/*
		msg = string_message(MARTA_REDUCE_NODO, na->nodo_base->red.ip, 2, na->nodo_base->red.puerto, na->nodo_base->id);
		enviar_mensaje(fd, msg);
		destroy_message(msg);*/


	}
	list_iterate(reduce->nodos_archivo, (void*) _enviar_reduce_a_job);
	//printf("Enviado todos los nodos-archivo OK");

	return 0;
}

t_nodo_archivo* nodo_archivo_create(void){
	t_nodo_archivo* new = malloc(sizeof(t_nodo_archivo));
	new->nodo_base = NULL;

	return new;
}

t_reduce* recibir_mensaje_reduce(int fd){
	t_msg* msg;
	t_nodo_archivo* na;
	t_reduce* reduce;
	int cant_reduces, i;

	msg = recibir_mensaje(fd);
	if (msg->header.id == REDUCE_INFO) {
		//envio el nombre resultado, red_id, job_id, cant_nodos_archivo
		//le paso null porque todavia no tengo el nb, viene en la proxima recibir_mensaje
		reduce = reduce_create(msg->argv[0], msg->argv[1], msg->stream,	recibir_mensaje_nodo_base(fd));
		reduce->final = msg->argv[3];
		cant_reduces = msg->argv[2];
	} else {
		//esto esta deprecated
		//si marta me manda el fin reduces salgo del while porque ya termino de enviar reduces
		if (msg->header.id == FIN_REDUCES){
			return NULL;//marta termino de mandar el ultimo reduce
		}

	}
	destroy_message(msg);

	printf("Comenzando a recibir los nodos-archivo. cant: %d\n", cant_reduces);
	for (i = 0; i < cant_reduces; i++) {
		na = NULL;
		na = nodo_archivo_create();

		//cargo el nombre del archivo a reducir
		msg = recibir_mensaje(fd);
		if (msg->header.id == ARCHIVO_A_REDUCIR) {
			strcpy(na->archivo, msg->stream);
		}
		destroy_message(msg);

		//cargo la info del nodo a donde conectarse
		na->nodo_base = recibir_mensaje_nodo_base(fd);

		//agrego el reduce a la lista de reduce
		//printf("Recibido nuevo reduce sobre %s en el nodo_id: %d %s:%d", na->archivo, na->nodo_base->id, na->nodo_base->red.ip, na->nodo_base->red.puerto);
		printf("Recibido nuevo reduce sobre %s en el nodo %s\n", na->archivo,	nodo_base_to_string(na->nodo_base));
		list_add(reduce->nodos_archivo, (void*) na);
	}

	printf("Fin recepcion de nodos-archivo\n");

	return reduce;
}

void print_map(t_map* map){
	printf("*************************************************************\n");
	printf("Map_id: %d - Ubicacion: id_nodo: %d, %s:%d numero_bloque: %d, resultado: %s\n",
			map->info->id,
			map->archivo_nodo_bloque->base->id,
			map->archivo_nodo_bloque->base->red.ip,
			map->archivo_nodo_bloque->base->red.puerto,
			map->archivo_nodo_bloque->numero_bloque,
			map->info->resultado);
	printf("*************************************************************\n");
}

int enviar_mensaje_script(int fd, char* path_script){
	char* script = file_get_mapped(path_script);
	t_msg* msg = string_message(JOB_SCRIPT, script, 0);
	enviar_mensaje(fd, msg);
	file_mmap_free(script, path_script);
	destroy_message(msg);
	return 0;
}

int recibir_mensaje_script_y_guardar(int fd, char* path_destino_script){
	t_msg* msg = NULL;
	//recibo el codigo del script
	msg = recibir_mensaje(fd);
	//guardo en el path destino el script
	write_file(path_destino_script,msg->stream, strlen(msg->stream));
	destroy_message(msg);
	//settear permisos de ejecucion
	chmod(path_destino_script, S_IRWXU);

	return 0;
}

t_map* recibir_mensaje_map(int fd){
	t_mapreduce* mr = NULL;
	mr = recibir_mensaje_mapreduce(fd);

	t_archivo_nodo_bloque* anb = NULL;
	anb = recibir_mensaje_archivo_nodo_bloque(fd);

	t_map* map = malloc(sizeof(t_map));
	map->archivo_nodo_bloque = anb;
	map->info = mr;

	return map;
}

/*
 * envia el mensaje con ID NODO_BASE
 */
int enviar_mensaje_nodo_base(int fd, t_nodo_base* nb){
	t_msg* msg = NULL;
	msg = nodo_base_message(nb);

	int rs = enviar_mensaje(fd, msg);
	destroy_message(msg);
	return rs;
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

t_nodo_base* nodo_base_new(int id, char* ip, int puerto){
	t_nodo_base* new = malloc(sizeof*new);
	new->id = id;
	strcpy(new->red.ip, ip);
	new->red.puerto = puerto;
	return new;
}

t_nodo_base* recibir_mensaje_nodo_base(int fd){
	t_msg* msg = recibir_mensaje(fd);
	t_nodo_base* nb = NULL;
	if(msg->header.id == NODO_BASE){
		nb = nodo_base_new(msg->argv[1], msg->stream, msg->argv[0]);
	}
	destroy_message(msg);
	return nb;
}

t_msg *recibir_mensaje(int sock_fd) {

	t_msg *msg = malloc(sizeof *msg);
	msg->argv = NULL;
	msg->stream = NULL;

	/* Get message info. */

	int status = recv(sock_fd, &(msg->header), sizeof(t_header), MSG_WAITALL);


	if (status < 0) {
		/* An error has ocurred or remote connection has been closed. */
		printf("error al recibir header sock %d\n", sock_fd);
		perror("rec header");
		FREE_NULL(msg);
		return NULL;
	}
	if(status == 0 ){
		printf("sock cerrado!%d\n", sock_fd);
		FREE_NULL(msg);
		return NULL;
	}


	/* Get message data. */
	if (msg->header.argc > 0) {
		msg->argv = malloc(msg->header.argc * sizeof(uint32_t));

		if (recv(sock_fd, msg->argv, msg->header.argc * sizeof(uint32_t), MSG_WAITALL) <= 0) {
			perror("rec args");
			FREE_NULL(msg->argv);
			FREE_NULL(msg);
			return NULL;
		}
	}

	if (msg->header.length > 0) {
		msg->stream = malloc(msg->header.length + 1);

		if (recv(sock_fd, msg->stream, msg->header.length, MSG_WAITALL) <= 0) {
			perror("rec stream");
			FREE_NULL(msg->stream);
			FREE_NULL(msg->argv);
			FREE_NULL(msg);
			return NULL;
		}

		msg->stream[msg->header.length] = '\0';
	}

	return msg;
}
int enviar_mensaje_nodo_ok(int fd){
	t_msg* msg = argv_message(NODO_OK, 0);

	int rs;
	rs = enviar_mensaje(fd, msg);
	destroy_message(msg);
	return rs;

}
int recibir_mensaje_nodo_ok(int fd){
	t_msg* msg = recibir_mensaje(fd);
	int rs ;
	rs = msg->header.id == NODO_OK;

	destroy_message(msg);
	return rs;
}

int enviar_mensaje_nodo_close(int fd){
	int rs ;
	t_msg* msg = argv_message(NODO_CHAU, 0);
	rs = enviar_mensaje(fd, msg);
	destroy_message(msg);



	//close(fd);
	return rs;
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
		//int sent = send(sock_fd, buffer, msg->header.length + sizeof msg->header + msg->header.argc * sizeof(uint32_t), MSG_NOSIGNAL);
		int sent = send(sock_fd, buffer, msg->header.length + sizeof msg->header + msg->header.argc * sizeof(uint32_t), 0);
		if (sent < 0) {
			free(buffer);
			return -1;
		}
		total += sent;
		pending -= sent;
	}
	/*
	while (total < pending) {
		//int sent = send(sock_fd, buffer, msg->header.length + sizeof msg->header	+ msg->header.argc * sizeof(uint32_t), MSG_NOSIGNAL);
		int sent = send(sock_fd, buffer, msg->header.length + sizeof msg->header	+ msg->header.argc * sizeof(uint32_t), 0);
		if (sent < 0) {
			printf("sock_fd %d error\n", sock_fd);
			perror("send:::");
			FREE_NULL(buffer);
			return -1;
		}
		total += sent;
		pending -= sent;
	}*/

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
	//if (msg->header.argc && msg->argv != NULL)
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
	case FS_AGREGO_NODO:
		buf = strdup("FS_AGREGO_NODO");
		break;
	case FS_ESTA_OPERATIVO:
		buf = strdup("FS_ESTA_OPERATIVO");
		break;
	case JOB_INFO:
		buf = strdup("JOB_INFO");
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

int split_count(char** splitted){
	int count;
	for(count=0;splitted[count]!=NULL;count++);
	return count;
}


void free_split(char** splitted){
	int i = 0;
	while (splitted[i] != NULL) {
		FREE_NULL(splitted[i]);
		i++;
	}
	FREE_NULL(splitted);
}

char NODO_BASE_PRINT[30];
char* nodo_base_to_string(t_nodo_base* nb){
	memset(NODO_BASE_PRINT, 0, 30);
	sprintf(NODO_BASE_PRINT, "id:%d, %s:%d", nb->id, nb->red.ip, nb->red.puerto);
	printf("%s\n", NODO_BASE_PRINT );
	return NODO_BASE_PRINT;
}


t_reduce* reduce_create(int id, int job_id, char* resultado, t_nodo_base* nb){
	t_reduce* new = malloc(sizeof*new);

	new->info = mapreduce_create(id,job_id, resultado);
	new->nodo_base_destino = nb;

	new->info->job_id = job_id;
	new->nodos_archivo = list_create();
	new->final = false;

	return new;
}



