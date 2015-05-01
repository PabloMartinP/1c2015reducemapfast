#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <util.h>

#include <commons/string.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <commons/log.h>
#include <pthread.h>
#include "config_nodo.h"

#include <nodo.h>

//#include "socket.h"

#define FILE_CONFIG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/config.txt"
#define FILE_LOG "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoNodo/log.txt"
/*
 * variables
 */


char* _data = NULL;
t_log* logger = NULL;
/*
 * declaraciones
 */
void* data_get(char* filename);
void data_destroy();
void* getBloque(int32_t numero);
void setBloque(int32_t numero, void* bloque);

void* getFileContent(char* filename);

void inicializar();
void finalizar();

void conectar_con_fs();
int NODO_CANT_BLOQUES();
/*
 * main
 */
int TAMANIO_DATA;
int CANT_BLOQUES;
int main(int argc, char *argv[]) {
	//por param le tiene que llegar el tamaño del archivo data.bin
	//por ahora hardcodeo 100mb, serian 10 bloques de 20 mb
	TAMANIO_DATA = 1024 * 1024 * 100; //100MB
	CANT_BLOQUES = TAMANIO_DATA / TAMANIO_BLOQUE_B;

	inicializar();

	conectar_con_fs();


	/*
	 * todo: test settear 0 bloque y leerlo
	 //settear el bloque 0
	 void* saludo = malloc(TAMANIO_BLOQUE);
	 strcpy(saludo, "ahora cambio el mensaje!");
	 setBloque(0, saludo);

	 //leo el bl0que 0
	 void* dataget = getBloque(0);
	 char* saludoget =(char*) malloc(strlen(saludo)+1);
	 memcpy(saludoget, dataget, strlen(saludo)+1);
	 printf("%s\n", saludoget);

	 free_null(saludo);
	 free_null(saludoget);
	 free_null(dataget);
	 */



	 /*
	  * todo: TEST LEER UN ARCHIVO DE LA CARPETA /TMP
	  * EL ARCHIVO DEBE ESTAR CREADO
	 int i=0;
	 char *d = NULL;
	 d = getFileContent("hola");
	 for(i=0;i<10;i++)
	 printf("%c", d[i]);
	 file_mmap_free(d, "hola");
	 */

////
	finalizar();

	return EXIT_SUCCESS;
}

int NODO_CANT_BLOQUES(){
	return CANT_BLOQUES;
}

void conectar_con_fs() {

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

			msg = string_message(RTA_FS_NODO_QUIEN_SOS, NODO_IP(), 3,NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES());
			if((enviar_mensaje(fs, msg))<0){
				printf("No se pudo responder a %s", id_string(FS_NODO_QUIEN_SOS) );
				exit(EXIT_FAILURE);
			}
			destroy_message(msg);


			printf("mensaje de ip y puerto enviado\n");

		} else
			printf("No se pudo conectar con el fs");

		close(fs);
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
	config = config_create(FILE_CONFIG);
	logger = log_create(FILE_LOG, "Nodo", true, LOG_LEVEL_INFO);

	_data = data_get(NODO_ARCHIVOBIN());

	//guardo la posicion de cada bloque en una nueva variable para no tener que hacer el calculo de NBLOQUE*20mb ...
	//bloques_set();
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

	free(path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;
}
/*
void bloques_set() {

	//creo el espacio para guardar las posiciones de memoria de cada
	_bloques = malloc(CANT_BLOQUES);

	int i = 0;
	for (i = 0; i < CANT_BLOQUES; i++) {
		//_bloques[i] = (_data + (i * TAMANIO_BLOQUE));
		(_bloques+i) = &i;
	}
}*/

void setBloque(int32_t numero, void* bloquedatos) {
	log_info(logger, "Inicio setBloque(%d)", numero);

	//char* b = (char*)malloc(sizeof(TAMANIO_BLOQUE));
	//b = bloquedatos;

	//memcpy((void*)(_data[numero*TAMANIO_BLOQUE]), bloquedatos, TAMANIO_BLOQUE);
	memcpy(_data+(numero*TAMANIO_BLOQUE_B), bloquedatos, TAMANIO_BLOQUE_B);

	log_info(logger, "Fin setBloque(%d)", numero);
}
/*
 * devuelve una copia del bloque, hacer free
 */
void* getBloque(int32_t numero) {
	log_info(logger, "Ini getBloque(%d)", numero);
	void* bloque = NULL;
	bloque = malloc(TAMANIO_BLOQUE_B);
	memcpy(bloque,&(_data[numero*TAMANIO_BLOQUE_B]), TAMANIO_BLOQUE_B);
	//memcpy(bloque, _bloques[numero], TAMANIO_BLOQUE);
	log_info(logger, "Fin getBloque(%d)", numero);
	return bloque;
}

//devuelvo el archivo data.bin mappeado
void* data_get(char* filename) {

	if (!file_exists(filename)) {
		FILE* file = NULL;
		file = fopen(filename, "w+");
		if (file == NULL) {
			handle_error("fopen");
		}

		printf("creado\n");
		//lo creo con el tamaño maximo
		void* dump = NULL;
		dump = malloc(TAMANIO_DATA);

		////grabo 0 en todo el nodo.
		memset(dump, 0, TAMANIO_DATA);
		fwrite(dump, TAMANIO_DATA, 1, file);
		free_null(dump);

		fclose(file);
	}

//el archivo ya esta creado con el size maximo
	return file_get_mapped(filename);
}

void data_destroy() {
	munmap(_data, TAMANIO_DATA);
//mapped = NULL;
}

