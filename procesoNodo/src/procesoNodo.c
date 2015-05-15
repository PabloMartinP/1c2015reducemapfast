#include <stdio.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <sys/mman.h>


#include <commons/string.h>
#include <commons/temporal.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>
#include "config_nodo.h"

#include <nodo.h>

#include <util.h>
//#include "socket.h"

char FILE_CONFIG [1024] = "/config.txt";
char FILE_LOG [1024] = "/log.txt";

/* since pipes are unidirectional, we need two pipes.
   one for data to flow from parent's stdout to child's
   stdin and the other for child's stdout to flow to
   parent's stdin */

#define NUM_PIPES          2

#define PARENT_WRITE_PIPE  0
#define PARENT_READ_PIPE   1

int pipes[NUM_PIPES][2];

/* always in a pipe[], pipe[0] is for read and
   pipe[1] is for write */
#define READ_FD  0
#define WRITE_FD 1

#define PARENT_READ_FD  ( pipes[PARENT_READ_PIPE][READ_FD]   )
#define PARENT_WRITE_FD ( pipes[PARENT_WRITE_PIPE][WRITE_FD] )

#define CHILD_READ_FD   ( pipes[PARENT_WRITE_PIPE][READ_FD]  )
#define CHILD_WRITE_FD  ( pipes[PARENT_READ_PIPE][WRITE_FD]  )

/*
 * variables
 */
bool FIN = false;
char* _data = NULL;
t_log* logger = NULL;
/*
 * declaraciones
 */
void* data_get(char* filename);
void data_destroy();
char* getBloque(int32_t numero);
void setBloque(int32_t numero, char* bloque);

void* getFileContent(char* filename);

void inicializar();
void finalizar();
void probar_conexion_fs();
void iniciar_server_thread();
int NODO_CANT_BLOQUES();
void procesar_mensaje(int fd, t_msg* msg);
void incicar_server();
void agregar_cwd(char* file);
/*
 * graba el temp concatenandole el timenow en el filename para que sea unico
 */
char* grabar_en_temp(char* filename, char* data);

/*
 * devuelve el archivo creado en el  temp del nodo
 */
char* aplicar_map(int n_bloque, char* mapping);
/*
 * main
 */
int TAMANIO_DATA;
int CANT_BLOQUES;
int main(int argc, char *argv[]) {
	//por param le tiene que llegar el tamaño del archivo data.bin
	//por ahora hardcodeo 100mb, serian 10 bloques de 20 mb


	inicializar();

	probar_conexion_fs();

	//inicio el server para atender las peticiones del fs
	iniciar_server_thread();

	int n_bloque =0;
	//aplicar_map(n_bloque, "/home/utnso/Escritorio/scripts/map.py");
	aplicar_map(n_bloque, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/map.sh");

	//bool fin = true	;
	//while (!FIN);
	finalizar();

	return EXIT_SUCCESS;
}

char* aplicar_map(int n_bloque, char* mapping){
	//int outfd[2];
	//int infd[2];


	// pipes for parent to write and read
	pipe(pipes[PARENT_READ_PIPE]);
	pipe(pipes[PARENT_WRITE_PIPE]);

	if (!fork()) {
		//char *argv[] = { mapping, NULL , 0 };
        char *argv[]={ mapping, NULL};


		dup2(CHILD_READ_FD, STDIN_FILENO);
		dup2(CHILD_WRITE_FD, STDOUT_FILENO);

		/* Close fds not required by child. Also, we don't
		 want the exec'ed program to know these existed */
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);
		close(PARENT_READ_FD);
		close(PARENT_WRITE_FD);

		execv(argv[0], argv);
		return NULL;
	} else {
		char* buffer = malloc(TAMANIO_BLOQUE_B);
		//char *buffer = malloc(100);
		char* new_file_mapped = NULL;
		int count;

		/* close fds not required by parent */
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);

		// Write to child’s stdin
		//char* stdinn = "2^10\n3Por2\n\0";
		char* stdinn = getBloque(n_bloque);
		size_t len = strlen(stdinn)+1;
		char* _stdin = malloc(len+1);
		memcpy(_stdin, stdinn, len+1);
		_stdin[len-1] = '\n';
		write(PARENT_WRITE_FD, _stdin, len+1);

		//spliteo por enter
		char** registros = string_split(_stdin, "\n");
		int c_registros = cant_registros(registros);
		// Read from child’s stdout
		int i=0, n_reg = 0;
		do{
			count = read(PARENT_READ_FD, buffer + i, 1);
			if(buffer[i]=='\n')
				n_reg++;
			i++;
		//}while(count!=0);
		}while(n_reg < c_registros );
        //count = read(PARENT_READ_FD, buffer, 100);

		if (i >= 0) {
			buffer[i] = 0;
			printf("%s\n", buffer);
			//grabo el archivo

			new_file_mapped = grabar_en_temp("job_map" ,buffer);


		} else {
			printf("IO Error\n");
		}

		free(buffer);
		return new_file_mapped;
	}

}

char* grabar_en_temp(char* filename, char* data){
	char* nombre_nuevo_archivo = NULL;
	nombre_nuevo_archivo = file_combine(NODO_DIRTEMP(), filename);
	char* timenow = temporal_get_string_time();
	string_append(&nombre_nuevo_archivo, timenow);
	free_null((void*) &timenow);

	write_file(nombre_nuevo_archivo, data, strlen(data));

	//free_null(&nombre_nuevo_archivo);
	return nombre_nuevo_archivo;
}

void agregar_cwd(char* file){
	char cwd[1024];
	if (getcwd(cwd, sizeof(cwd)) == NULL)
		handle_error("getcwd() error");

	char* aux = malloc(strlen(file));
	strcpy(aux, file);
	strcpy(file, cwd);
	strcat(file, aux);
	free(aux);
}


void iniciar_server_thread() {
	pthread_t th;
	pthread_attr_t tattr;

	pthread_attr_init(&tattr);
	pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
	pthread_create(&th, &tattr, (void*) incicar_server, NULL);
	pthread_attr_destroy(&tattr);

}

void procesar_mensaje(int fd, t_msg* msg) {
	print_msg(msg);
	int n_bloque = 0;

	switch (msg->header.id) {
	case NODO_GET_BLOQUE:
		n_bloque = msg->argv[0];
		destroy_message(msg);
		msg = string_message(NODO_GET_BLOQUE, getBloque(n_bloque), 0);//en la posicion 0 esta en nuemro de bloque
		enviar_mensaje(fd, msg);

		destroy_message(msg);

		break;
	case NODO_HOLA:
		destroy_message(msg);
		msg = string_message(NODO_HOLA, "", 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);
		break;
	case NODO_CHAU:
		FIN = true;
		destroy_message(msg);

		break;
	case FS_HOLA:

		destroy_message(msg);
		msg = string_message(NODO_HOLA, "", 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		//recibo el mensaje con el bloque a grabar, y la posiocion en el arg 0
		msg = recibir_mensaje(fd);
		print_msg(msg);

		char* bloque = malloc(TAMANIO_BLOQUE_B);
		memcpy(bloque, msg->stream, msg->argv[1]);	//1 es el tamaño real
		memset(bloque + msg->argv[1], '\0', TAMANIO_BLOQUE_B - msg->argv[1]);
		setBloque(msg->argv[0], bloque);
		free_null((void*)&bloque);

		destroy_message(msg);

		break;
	default:
		break;
	}

}
void incicar_server() {
	printf("Iniciado server nodo para FS y JOB. Puerto: %d\n", NODO_PORT());

	server_socket_select(NODO_PORT(), procesar_mensaje);
}

int NODO_CANT_BLOQUES() {
	return CANT_BLOQUES;
}

void probar_conexion_fs() {

	log_trace(logger, "conectado al FS ... ");

	int fs;

	if ((fs = client_socket(NODO_IP_FS(), NODO_PORT_FS())) > 0) {
		printf("Conectado\n");
		//t_msg* msg= id_message(NODO_CONECTAR_CON_FS);
		t_msg* msg = NULL;
		msg = string_message(NODO_CONECTAR_CON_FS, "hola soy el NodoA", 0);
		printf("enviavndo mensaje al fs\n");
		if (enviar_mensaje(fs, msg) < 0) {
			puts("ERROR: Se ha perdido la conexión con el fs.");
			exit(EXIT_FAILURE);
		}
		destroy_message(msg);

		printf("recibiendo respueta\n");
		if ((msg = recibir_mensaje(fs)) == NULL) {
			puts("ERROR: Se ha perdido la conexión con el fs.\n");
			exit(EXIT_FAILURE);
		}

		print_msg(msg);
		if (msg->header.id == FS_NODO_QUIEN_SOS) {
			printf("el fs me pregunta quien soy, le digo mi ip y port\n");

			destroy_message(msg);

			/*
			 t_nodo* nodo = nodo_new(NODO_IP(), NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES());
			 enviar_mensaje_flujo(fs, 1, sizeof(t_nodo), nodo);*/

			msg = string_message(RTA_FS_NODO_QUIEN_SOS, NODO_IP(), 3,
					NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES());
			if ((enviar_mensaje(fs, msg)) < 0) {
				printf("No se pudo responder a %s",
						id_string(FS_NODO_QUIEN_SOS));
				exit(EXIT_FAILURE);
			}
			destroy_message(msg);

			printf("mensaje de ip y puerto enviado\n");

		} else
			printf("No se pudo conectar con el fs");

		close(fs);
		printf("Conectado con fs en %s:%d\n", NODO_IP_FS(), NODO_PORT_FS());
	} else {
		printf("No pudo iniciar la escucha al fs\n");
	}

}
void finalizar() {
	data_destroy();
	config_destroy(config);
	log_destroy(logger);

	printf("fin ok\n");
	//while (true);
}

void inicializar() {
	/*
	 * deberia tomar el getcwd y concatenarle el nombre de archivo
	 char cwd[1024];
	 getcwd(cwd, sizeof(cwd));
	 char* file_config = file_combine(cwd, FILE_CONFIG);
	 */
	agregar_cwd(FILE_CONFIG);
	config = config_create(FILE_CONFIG);
	agregar_cwd(FILE_LOG);
	logger = log_create(FILE_LOG, "Nodo", true, LOG_LEVEL_INFO);

	_data = data_get(NODO_ARCHIVOBIN());
}

/*
 * devuelve un puntero con el archivo mapeado
 */
void* getFileContent(char* filename) {
	log_info(logger, "Inicio getFileContent(%s)", filename);
	void* content = NULL;

//creo el espacio para almacenar el archivo
	char* path = file_combine(NODO_DIRTEMP(), filename);
	content = malloc(file_get_size(path));

	content = file_get_mapped(path);

	free_null((void*)&path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;
}

void setBloque(int32_t numero, char* bloquedatos) {
	log_info(logger, "Inicio setBloque(%d)", numero);

	//memcpy((void*)(_data[numero*TAMANIO_BLOQUE]), bloquedatos, TAMANIO_BLOQUE);
	memcpy(_data + (numero * TAMANIO_BLOQUE_B), bloquedatos, TAMANIO_BLOQUE_B);

	log_info(logger, "Fin setBloque(%d)", numero);
}
/*
 * devuelve una copia del bloque, hacer free
 */
char* getBloque(int32_t numero) {
	log_info(logger, "Ini getBloque(%d)", numero);
	void* bloque = NULL;
	bloque = malloc(TAMANIO_BLOQUE_B);
	memcpy(bloque, &(_data[numero * TAMANIO_BLOQUE_B]), TAMANIO_BLOQUE_B);
	//memcpy(bloque, _bloques[numero], TAMANIO_BLOQUE);
	log_info(logger, "Fin getBloque(%d)", numero);
	return bloque;
}

//devuelvo el archivo data.bin mappeado
void* data_get(char* filename) {
	char filenamenew[1024];
	strcpy(filenamenew, filename);
	agregar_cwd(filenamenew);

	if (!file_exists(filenamenew)) {
		TAMANIO_DATA = 1024 * 1024 * 500; //100MB
		FILE* file = NULL;
		file = fopen(filenamenew, "w+");
		if (file == NULL) {
			handle_error("fopen");
		}

		printf("creando archivo de %d bytes ...\n", TAMANIO_DATA);
		//lo creo con el tamaño maximo
		void* dump = NULL;
		dump = malloc(TAMANIO_DATA);

		////grabo 0 en todo el nodo.
		memset(dump, 0, TAMANIO_DATA);
		fwrite(dump, TAMANIO_DATA, 1, file);
		free_null((void*)&dump);

		fclose(file);
	}
	//calculo la cantidad de bloques
	TAMANIO_DATA = file_get_size(filenamenew);
	CANT_BLOQUES = TAMANIO_DATA / TAMANIO_BLOQUE_B;

//el archivo ya esta creado con el size maximo
	return file_get_mapped(filenamenew);
}

void data_destroy() {
	munmap(_data, TAMANIO_DATA);
//mapped = NULL;
}


