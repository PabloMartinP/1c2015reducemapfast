#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <commons/string.h>
#include <pthread.h>
#include <strings.h>

#define LEN_KEYVALUE 1024
#define NUM_PIPES          2

#define PARENT_WRITE_PIPE  0
#define PARENT_READ_PIPE   1

#define READ_FD  0
#define WRITE_FD 1


typedef struct{
	char *origen;
	char *destino;
}t_ordenar;

#define CANT_THREADS 50
pthread_mutex_t mutex;

#define handle_error(msj) \
	do{perror(msj);exit(EXIT_FAILURE);} while(0)

void escribir_todo(int writer, char* data, int len);
int ordenar(t_ordenar* param_ordenar);
int ejecutar_script(char* path_script, char* name_script, int(*reader_writer)(int fdreader, int fdwriter));
int main(void){
	pthread_mutex_init(&mutex, NULL);
	char* desordenado= "/home/utnso/Escritorio/tests/5.txt";
	//char* nombre_destino =  "/tmp/sort-res-";
	int i;
	pthread_t th[CANT_THREADS];
	t_ordenar* p_ordenar;
	for (i = 0; i < CANT_THREADS; i++) {
		p_ordenar = malloc(sizeof(t_ordenar));
		p_ordenar->origen = string_from_format("%s", desordenado);
		p_ordenar->destino =string_from_format("%s%d.txt", "/tmp/sort-res-", i);

		pthread_create(&th[i], NULL, (void*)ordenar, (void*)p_ordenar);
	}

	for (i = 0; i < CANT_THREADS; i++) {
		pthread_join(th[i], NULL);
	}
	pthread_mutex_destroy(&mutex);
	puts("finnnnnnnnnnnnnnnnnn");


	return 0;
}


int ejecutar_script(char* path_script, char* name_script, int(*reader_writer)(int fdreader, int fdwriter)){
	pthread_mutex_lock(&mutex);

	int pipes[NUM_PIPES][2];
	// pipes for parent to write and read

	if (pipe(pipes[PARENT_READ_PIPE]) == -1)
		handle_error("pipe");

	if ((pipe(pipes[PARENT_WRITE_PIPE])) == -1)
		handle_error("pipe");

	///////////////////////////////////////////
	int p;
	p = fork();
	pthread_mutex_unlock(&mutex);
	if (p < 0)
		handle_error("fork pipe stdin stdout");

	if (p == 0) {
		if (dup2(pipes[PARENT_WRITE_PIPE][READ_FD], STDIN_FILENO) < 0) {
			perror("dup2 STDIN_FILENO");
			exit(0);
		}
		if (dup2(pipes[PARENT_READ_PIPE][WRITE_FD], STDOUT_FILENO) < 0) {
			perror("dup2 STDIN_FILENO");
			exit(-1);
		}

		close(pipes[PARENT_WRITE_PIPE][READ_FD]);
		close(pipes[PARENT_READ_PIPE][WRITE_FD]);
		close(pipes[PARENT_READ_PIPE][READ_FD]);
		close(pipes[PARENT_WRITE_PIPE][WRITE_FD]);

		//execl("/usr/bin/sort", "sort", (char*) NULL);
		execl(path_script, name_script, (char*) NULL);
		perror("Errro execv");
		exit(-1);

	} else {
		close(pipes[PARENT_WRITE_PIPE][READ_FD]);
		close(pipes[PARENT_READ_PIPE][WRITE_FD]);

		int rs;
		rs = reader_writer(pipes[PARENT_READ_PIPE][READ_FD], pipes[PARENT_WRITE_PIPE][WRITE_FD]);
		int w;
		wait(&w);

		puts("listo");
		return rs;
	}
}

int ordenar(t_ordenar* param_ordenar){
	int rs;

	int _reader_writer(int fdreader, int fdwriter){

		void _writer(int fd) {

			FILE* file = fopen(param_ordenar->origen , "r");
			if(file==NULL){
				handle_error("fopen");
			}
			char* linea = NULL;
			linea = malloc(LEN_KEYVALUE);
			size_t len_linea = LEN_KEYVALUE;
			int rs;
			while ((rs = getline(&linea, &len_linea, file)) > 0) {
				escribir_todo(fd, linea, strlen(linea));
				//write(pipes[PARENT_WRITE_PIPE][WRITE_FD], linea, strlen(linea));
				//(pipes[PARENT_WRITE_PIPE][WRITE_FD], linea, strlen(linea));
			}
			free(linea);
			fclose(file);
			close(fd);
		}

		void _reader(int fd) {
			int count, rs;
			FILE* file = fopen(param_ordenar->destino, "w");
			if(file==NULL){
				handle_error("fopen");
			}
			char buffer[LEN_KEYVALUE];
			while ((count = read(fd, buffer, LEN_KEYVALUE)) > 0) {
				rs = fwrite(buffer, 1, count, file);
				//printf("%s\n", buffer);
				if (rs <= 0) {
					perror("_________fwrite");
					exit(-1);
				}
			}
			if(count<0){
				handle_error("read");
			}
			fclose(file);
			close(fd);
		}

		pthread_t th_writer, th_reader;
		pthread_create(&th_writer, NULL, (void*) _writer, (void*)fdwriter);
		pthread_create(&th_reader, NULL, (void*) _reader, (void*)fdreader);

		pthread_join(th_writer, NULL);
		pthread_join(th_reader, NULL);

		free(param_ordenar->destino);
		free(param_ordenar->origen);
		free(param_ordenar);
		return 0;
	}

	rs = ejecutar_script("/usr/bin/sort", "sort", _reader_writer);
	//rs = ejecutar_script("/home/utnso/Escritorio/tests/mapper.sh", "mapperR", _reader_writer);

	return rs;
}

void escribir_todo(int writer, char* data, int len){
	int aux = 0;
	int bytes_escritos = 0;
	do {
		aux = write(writer, data + bytes_escritos, len - bytes_escritos);
		//fprintf(stdout, "bytesEscritos: %d\n", aux);
		if (aux < 0) {
			printf("_____________write Error\n");
			perror("write:::::::::::::::::::::::");
			exit(-1);
		}
		bytes_escritos = bytes_escritos + aux;
	} while (bytes_escritos < len);
	//fsync(pipes[PARENT_WRITE_PIPE][WRITE_FD]);
}
