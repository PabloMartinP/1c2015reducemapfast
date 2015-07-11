
#include "procesoNodo.h"


typedef struct {
	int numero_bloque;
	char script_map[255];
	char resultado[255];
} th_map;

int contador_ftok=0;//para inicializar el semaforo(?)

int main(int argc, char *argv[]) {
	//por param le tiene que llegar el tamaño del archivo data.bin
	//por ahora hardcodeo 100mb, serian 10 bloques de 20 mb


	inicializar();

	probar_conexion_fs();

	signal(SIGCHLD, SIG_IGN);
	//inicio el server para atender las peticiones del fs
	//iniciar_server_thread();
	//iniciar_server();
	incicar_server_sin_select();
	//iniciar_server_fork();


	//bool fin = true	;
	finalizar();

	return EXIT_SUCCESS;
}

int aplicar_reduce_ok(t_list* files_reduces, char*script_reduce,	char* filename_result, pthread_mutex_t* mutex) {

	int rs;
	int _reader_writer(int fdreader, int fdwriter) {
		int _writer(int fd) {
			int rs;
			int cant_red_files, cant_total_files, cant_local_files;
			t_list* local_files = list_create();//tmp para guardar los fr locales
			t_list* local_files_reduce;	// = list_create();	//lista que solo tiene los nombres de los archivos temp
			t_list* red_files;	// = list_create();

			//creo ambas listas
			local_files_reduce = list_filter(files_reduces,
					(void*) file_reduce_es_local);
			red_files = list_filter(files_reduces,
					(void*) file_reduce_es_de_red);

			//cargo la lista local_files solo con los path absolutos
			void _convertir_a_absoluto(t_files_reduce* fr) {
				list_add(local_files,
						convertir_a_temp_path_filename(fr->archivo));
			}
			list_iterate(local_files_reduce, (void*) _convertir_a_absoluto);

			cant_total_files = list_size(files_reduces);
			//leo la cantidad de cada lista
			cant_red_files = list_size(red_files);
			cant_local_files = list_size(local_files);

			//creo un descriptor de archivo por cada file local
			FILE** fdlocal = malloc(cant_local_files * sizeof(FILE*));
			//abro los archivos locales
			int i = 0;
			void _open_file(char* filename) {
				fdlocal[i] = fopen(filename, "r");
				if (fdlocal[i] == NULL) {
					printf("ERRROR fopen r");
					exit(1);
				}
				i++;
			}
			list_iterate(local_files, (void*) _open_file);

			//hasta aca tengo los archivos locales abiertos, listos para empezar a leer
			//ahora tengo que decirle a los nodos que stan en red que me devuelvan su archivo para empezara leer
			int* fdred = malloc(cant_red_files * sizeof(int));
			i = 0;
			void _request_file_to_nodo(t_files_reduce* fr) {
				fdred[i] = client_socket(fr->ip, fr->puerto);
				t_msg* msg = string_message(NODO_GET_FILECONTENT_DATA,
						fr->archivo, 0);
				//envio el mensaje al nodo pidiendole el archivo
				enviar_mensaje(fdred[i], msg);
				destroy_message(msg);
				i++;
			}
			list_iterate(red_files, (void*) _request_file_to_nodo);

			//hasta aca ya le pedi a todos los nodos que me devuelvan el archivo, estan esperando que los lea
			//ahora me queda leer tanto los fdlocal como los fdred

			//limpio las listas que ya no las voy a usar
			//list_destroy(local_files);
			list_destroy_and_destroy_elements(local_files, (void*) free);
			list_destroy(red_files);
			list_destroy(local_files_reduce);

			int bytes_escritos; //para el do write
			int index_menor; //para guardar el menor item
			//creo una lista de key para guardar las key de cada file
			size_t len_key = LEN_KEYVALUE;
			char* key = NULL;
			char** keys = malloc(sizeof(char*) * (cant_total_files));

			i = 0;
			//cargo las keys de los archivos locales, con su primer valor
			for (i = 0; i < cant_local_files; i++) {
				//rs = getline(&(keys[i]), &len_key, fd[i]);
				key = malloc(LEN_KEYVALUE);
				len_key = LEN_KEYVALUE;
				rs = getline(&key, &len_key, fdlocal[i]);
				if (rs != -1) {
					//key[rs] = '\0';
					keys[i] = key;
				}
			}
			//ahora me queda cargar las key que falta, que son las que estan en fdred
			//el i empieza donde quedo el anterior
			int j = 0;
			for (j = 0; i < cant_total_files; j++, i++) {
				keys[i] = recibir_linea(fdred[j]);
				//rs = getline(&(keys[i]), &len_key, fdlocal[i]);
			}
			//aca ya tengo todas las keys

			//empiezo a insertar en stdin
			i = 0;
			int c = 0;
			while (alguna_key_distinta_null(keys, cant_total_files)) {
				//obtengo cual es el menor
				index_menor = get_index_menor(keys, cant_total_files);
				//el menor lo mando a stdinn (keys[i])
				//fprintf(stdout, "%s\n", keys[index_menor]);

				rs = escribir_todo(fd, keys[index_menor], strlen(keys[index_menor]));
				/*pthread_mutex_lock(&mx_mr);
				aux = 0;
				bytes_escritos = 0;
				do {
					//aux = write(pipes[PARENT_WRITE_PIPE][WRITE_FD],	stdinn + bytes_leidos, len_buff_write - aux);
					aux = write(pipes[PARENT_WRITE_PIPE][WRITE_FD],
							keys[index_menor] + bytes_escritos,
							strlen(keys[index_menor]) - bytes_escritos);
					//fprintf(stdout, "bytesEscritos: %d\n", aux);
					bytes_escritos = bytes_escritos + aux;
				} while (aux != strlen(keys[index_menor]));
				//fprintf(stdout, "*************total: %d\n", bytes_escritos);
				rs = bytes_escritos;
				pthread_mutex_unlock(&mx_mr);*/

				//fprintf(stdout, "res write. %d\n", rs);
				//fprintf(stdout, "strlen: %d\n", strlen(keys[index_menor]));

				//leo el siguiente elmento del fdlocal[index_menor]
				len_key = LEN_KEYVALUE;
				if (index_menor < cant_local_files) {
					rs = getline(&(keys[index_menor]), &len_key, fdlocal[index_menor]);
					//si es igual a -1, termino el file, marco como null la key para que la ignore cuando obtiene el menor
					if (rs == -1) {
						FREE_NULL(keys[index_menor]);
						keys[index_menor] = NULL;
					}
				} else {
					FREE_NULL(keys[index_menor]);
					keys[index_menor] = recibir_linea( fdred[cant_local_files - index_menor]);
				}

				//cuando termina devuelve NULL;
				if (i > 1024) {
					i = 0;
					if (c % 100 == 0)
						fprintf(stdout, "Contador %d\n", c);

					c++;
				}
				i++;
			}

			pthread_mutex_lock(mutex);
			log_trace(logger, "Termino de enviarle datos por stdin.------------------");
			pthread_mutex_unlock(mutex);

			//si llego hasta aca termino de enviarle cosas por stdin,
			//cierro el stdin
			puts("antes de cerrar");
			close(fd);

			//cierro los archivos locales
			for (i = 0; i < cant_local_files; i++) {
				fclose(fdlocal[i]);
				//free(fdlocal[i]);
			}
			//borro fdlocal
			//free(fdlocal);
			FREE_NULL(fdlocal);

			//cierro las conexiones con lso nodos
			for (i = 0; i < cant_red_files; i++)
				close(fdred[i]);
			//borro fdred
			//free(fdred);
			FREE_NULL(fdred);

			FREE_NULL(keys);


			return 0;

		}//fin writer

		//lanzo hilo writer stdin;
		pthread_t th_writer, th_reader;
		pthread_create(&th_writer, NULL, (void*) _writer, (void*) fdwriter);

		//creo y lanzo el hilo reader
		t_reader treader;
		treader.fd = fdreader;
		char* result_reduce = convertir_a_temp_path_filename( filename_result);
		treader.destino = result_reduce;

		pthread_create(&th_reader, NULL, (void*) reader_and_save_as, (void*) &treader);

		//lockeo hasta que termine de escribir y leer el stdin y stdout
		pthread_join(th_writer, NULL);
		pthread_join(th_reader, NULL);

		//hasta aca ya tengo el map del archivo
		//me falta ordenarlo

		pthread_mutex_lock(mutex);
		log_trace(logger, "Fin lectura stdout resultado reduce, guardado en archivo %s", result_reduce);
		pthread_mutex_unlock(mutex);

		puts("reduce Fin hilo lectura stdout\n");

		FREE_NULL(result_reduce);
		return 0;
	}
	rs = ejecutar_script(script_reduce, "reduce_", _reader_writer, mutex);

	return rs;
}

//files es una lista de t_files_reduce
int aplicar_reduce_local_red(t_list* files_reduces, char*script_reduce,	char* filename_result) {

	key_t shmkey; /*      shared memory key       */
	int shmid; /*      shared memory id        */
	sem_t *sem; /*      synch semaphore         *//*shared */
	sem = sem_crear(&shmid, &shmkey);

	//key_t shmkey_read; /*      shared memory key       */
	//int shmid_read; /*      shared memory id        */
	//sem_t *sem_read; /*      synch semaphore         *//*shared */
	//sem_read = sem_crear(&shmid_read, &shmkey_read);
	sem_t sem_read;
	sem_init(&sem_read, 0, 0);

	///////////////////////////////////
	int aux;
	pthread_mutex_t mx_mr;/* mutex para que no haya deadlock por el fork*/
	pthread_mutex_init(&mx_mr, NULL);
	int pipes[NUM_PIPES][2];

	int _ejecutar_script(char* script, int (*procesar_std)()) {
		// pipes for parent to write and read

		if (pipe(pipes[PARENT_READ_PIPE]) == -1)
			handle_error("pipe");

		if ((pipe(pipes[PARENT_WRITE_PIPE])) == -1)
			handle_error("pipe");

		///////////////////////////////////////////

		int p = fork();
		if (p < 0)
			handle_error("fork pipe stdin stdout");

		if (p == 0) {
			//hijo
			/*pthread_mutex_t mutex;
			 pthread_mutex_init(&mutex, NULL);
			 pthread_mutex_lock(&mutex);*/
			char *argv[] = { script, NULL };
			//fflush(stdin);
			if (dup2(pipes[PARENT_WRITE_PIPE][READ_FD], STDIN_FILENO) < 0) {
				perror("dup2 STDIN_FILENO");
				exit(0);
			}
			//fflush(stdout);
			if (dup2(pipes[PARENT_READ_PIPE][WRITE_FD], STDOUT_FILENO) < 0) {
				perror("dup2 STDIN_FILENO");
				exit(0);
			}

			close(pipes[PARENT_WRITE_PIPE][READ_FD]);
			close(pipes[PARENT_READ_PIPE][WRITE_FD]);
			close(pipes[PARENT_READ_PIPE][READ_FD]);
			close(pipes[PARENT_WRITE_PIPE][WRITE_FD]);
			//close(CHILD_READ_FD);
			//close(CHILD_WRITE_FD);
			//close(PARENT_READ_FD);
			//close(PARENT_WRITE_FD);

			/*pthread_mutex_unlock(&mutex);
			 pthread_mutex_destroy(&mutex);*/
			sem_post(sem);
			execv(argv[0], argv);

			perror("Errro execv");
			printf("argv[0]:%s\n", argv[0]);
			exit(0);

			return 0;
		} else {
			//waitpid(p, NULL, NULL);
			printf("*************************************ANtes wait\n");
			sem_wait(sem);
			printf("*************************************Despues wait\n");

			close(pipes[PARENT_WRITE_PIPE][READ_FD]);
			close(pipes[PARENT_READ_PIPE][WRITE_FD]);

			sem_post(&sem_read);
			int rs;
			rs = procesar_std();

			//pthread_t th_stdin;
			//pthread_create(&th_stdin, NULL, (void*)procesar_std(), NULL);
			//pthread_join(th_stdin, (void*)&rs);
			//int status=0;
			//wait(&status);

			puts("listo");

			//wait(0);
			return rs;

		}
		return 0;
	}
	/*
	 * reduce********************************************************
	 */

	int _reduce_local_red() {



		int rs;
		int cant_red_files, cant_total_files, cant_local_files;
		t_list* local_files = list_create();//tmp para guardar los fr locales
		t_list* local_files_reduce;	// = list_create();	//lista que solo tiene los nombres de los archivos temp
		t_list* red_files;	// = list_create();

		//creo ambas listas
		local_files_reduce = list_filter(files_reduces,
				(void*) file_reduce_es_local);
		red_files = list_filter(files_reduces, (void*) file_reduce_es_de_red);
		/*
		 list_add_all(local_files_reduce,
		 list_filter(files_reduces, (void*) file_reduce_es_local));
		 list_add_all(red_files,
		 list_filter(files_reduces, (void*) file_reduce_es_de_red));
		 */
		//cargo la lista local_files solo con los path absolutos
		void _convertir_a_absoluto(t_files_reduce* fr) {
			list_add(local_files, convertir_a_temp_path_filename(fr->archivo));
		}
		list_iterate(local_files_reduce, (void*) _convertir_a_absoluto);

		cant_total_files = list_size(files_reduces);
		//leo la cantidad de cada lista
		cant_red_files = list_size(red_files);
		cant_local_files = list_size(local_files);

		//creo un descriptor de archivo por cada file local
		FILE** fdlocal = malloc(cant_local_files * sizeof(FILE*));
		//abro los archivos locales
		int i = 0;
		void _open_file(char* filename) {
			fdlocal[i] = fopen(filename, "r");
			if (fdlocal[i] == NULL) {
				printf("ERRROR fopen r");
				exit(1);
			}
			i++;
		}
		list_iterate(local_files, (void*) _open_file);

		//hasta aca tengo los archivos locales abiertos, listos para empezar a leer
		//ahora tengo que decirle a los nodos que stan en red que me devuelvan su archivo para empezara leer
		int* fdred = malloc(cant_red_files * sizeof(int));
		i = 0;
		void _request_file_to_nodo(t_files_reduce* fr) {
			fdred[i] = client_socket(fr->ip, fr->puerto);
			t_msg* msg = string_message(NODO_GET_FILECONTENT_DATA, fr->archivo,
					0);
			//envio el mensaje al nodo pidiendole el archivo
			enviar_mensaje(fdred[i], msg);
			destroy_message(msg);
			i++;
		}
		list_iterate(red_files, (void*) _request_file_to_nodo);

		//hasta aca ya le pedi a todos los nodos que me devuelvan el archivo, estan esperando que los lea
		//ahora me queda leer tanto los fdlocal como los fdred

		//limpio las listas que ya no las voy a usar
		//list_destroy(local_files);
		list_destroy_and_destroy_elements(local_files, (void*) free);
		list_destroy(red_files);
		list_destroy(local_files_reduce);

		int bytes_escritos; //para el do write
		int index_menor; //para guardar el menor item
		//creo una lista de key para guardar las key de cada file
		size_t len_key = LEN_KEYVALUE;
		char* key = NULL;
		char** keys = malloc(sizeof(char*) * (cant_total_files));


		i = 0;
		//cargo las keys de los archivos locales, con su primer valor
		for (i = 0; i < cant_local_files; i++) {
			//rs = getline(&(keys[i]), &len_key, fd[i]);
			key = malloc(LEN_KEYVALUE);
			len_key = LEN_KEYVALUE;
			rs = getline(&key, &len_key, fdlocal[i]);
			if (rs != -1) {
				//key[rs] = '\0';
				keys[i] = key;
			}
		}
		//ahora me queda cargar las key que falta, que son las que estan en fdred
		//el i empieza donde quedo el anterior
		int j = 0;
		for (j = 0; i < cant_total_files; j++, i++) {
			keys[i] = recibir_linea(fdred[j]);
			//rs = getline(&(keys[i]), &len_key, fdlocal[i]);
		}
		//aca ya tengo todas las keys


		//empiezo a insertar en stdin
		i = 0;
		int c = 0;
		while (alguna_key_distinta_null(keys, cant_total_files)) {
			//obtengo cual es el menor
			index_menor = get_index_menor(keys, cant_total_files);
			//el menor lo mando a stdinn (keys[i])
			//fprintf(stdout, "%s\n", keys[index_menor]);



			//rs = write(pipes[PARENT_WRITE_PIPE][WRITE_FD] , keys[index_menor], strlen(keys[index_menor]) );
			pthread_mutex_lock(&mx_mr);
			aux = 0;
			bytes_escritos = 0;
			do {
				//aux = write(pipes[PARENT_WRITE_PIPE][WRITE_FD],	stdinn + bytes_leidos, len_buff_write - aux);
				aux = write(pipes[PARENT_WRITE_PIPE][WRITE_FD] , keys[index_menor] + bytes_escritos, strlen(keys[index_menor]) - bytes_escritos);
				//fprintf(stdout, "bytesEscritos: %d\n", aux);
				bytes_escritos = bytes_escritos + aux;
			} while (aux != strlen(keys[index_menor]));
			//fprintf(stdout, "*************total: %d\n", bytes_escritos);
			rs = bytes_escritos;
			pthread_mutex_unlock(&mx_mr);

			//fprintf(stdout, "res write. %d\n", rs);
			//fprintf(stdout, "strlen: %d\n", strlen(keys[index_menor]));


			//leo el siguiente elmento del fdlocal[index_menor]
			len_key = LEN_KEYVALUE;
			if (index_menor < cant_local_files) {
				rs = getline(&(keys[index_menor]), &len_key,
						fdlocal[index_menor]);
				//si es igual a -1, termino el file, marco como null la key para que la ignore cuando obtiene el menor
				if (rs == -1) {
					FREE_NULL(keys[index_menor]);
					keys[index_menor] = NULL;
				}
			} else {
				FREE_NULL(keys[index_menor]);
				keys[index_menor] = recibir_linea(fdred[cant_local_files - index_menor]);
			}

			//cuando termina devuelve NULL;
			if (i > 1024) {
				i = 0;
				if(c % 100 == 0)
					fprintf(stdout, "Contador %d\n", c);

				c++;
			}
			i++;
		}

		pthread_mutex_lock(&mx_mr);
		log_trace(logger, "Termino de enviarle datos por stdin.------------------");
		pthread_mutex_unlock(&mx_mr);


		//si llego hasta aca termino de enviarle cosas por stdin,
		//cierro el stdin
		puts("antes de cerrar");
		close(pipes[PARENT_WRITE_PIPE][WRITE_FD] );

		//cierro los archivos locales
		for (i = 0; i < cant_local_files; i++) {
			fclose(fdlocal[i]);
			//free(fdlocal[i]);
		}
		//borro fdlocal
		//free(fdlocal);
		FREE_NULL(fdlocal);

		//cierro las conexiones con lso nodos
		for (i = 0; i < cant_red_files; i++)
			close(fdred[i]);
		//borro fdred
		//free(fdred);
		FREE_NULL(fdred);

		FREE_NULL(keys);

		pthread_mutex_destroy(&mx_mr);
		return 0;
	}

	//lanzo hilo para leer mientras escribe en stdin para que no se bloquee en el read
	pthread_t th_read;
	//int i = 0;
	int _read_reduce() {
		char* new_file_reduced;
		new_file_reduced = convertir_a_temp_path_filename(filename_result);	//genero filename absoluto
		FILE* file_reduced = txt_open_for_append(new_file_reduced);	//creo el file
		FREE_NULL(new_file_reduced);		//limpio el nombre
		//char* buffer = malloc(LEN_KEYVALUE);//creo un buffer para ir almacenando el stdout
		char buffer[LEN_KEYVALUE];

		int count;
		sem_wait(&sem_read);
		fprintf(stdout, "Comienzo a leer del hilo\n");
		while ((count = read(pipes[PARENT_READ_PIPE][READ_FD], buffer, LEN_KEYVALUE)) > 0) {
			//guardo en el archivo resultado
			fwrite(buffer, count, 1, file_reduced);
			//fprintf(stdout, "count: %d, res: %s\n", count, buffer);
			memset(buffer, 0, LEN_KEYVALUE);
		}
		if (count == -1) {
			perror("errroread");
		}
		fclose(file_reduced);

		puts("reduce fin hilo reader");
		return 0;

	}

	/*
pthread_create(&pth_read, NULL, (void*) _read, NULL);
	int rs ;
	pthread_t pth_write;
	void _writemap(){
		rs =  _ejecutar_script(script_map, (void*) __aplicar_map);
	}
	pthread_create(&pth_write, NULL, (void*)_writemap, NULL);
	pthread_join(pth_write, NULL);
	pthread_join(pth_read, NULL);

*/
	int rs;
	pthread_create(&th_read, NULL, (void*) _read_reduce, NULL);
	//rs =  _ejecutar_script(script_reduce, (void*) _reduce_local_red);
	pthread_t pth_write;
	void _writereduce() {
		rs =  _ejecutar_script(script_reduce, (void*) _reduce_local_red);
	}
	pthread_create(&pth_write, NULL, (void*) _writereduce, NULL);
	pthread_join(pth_write, NULL);

	pthread_join(th_read, NULL);

	close(pipes[PARENT_READ_PIPE][READ_FD]);

	/* shared memory detach */
	shmctl(shmid, IPC_RMID, 0);
		/* cleanup semaphores */
	sem_destroy (sem);


	/* cleanup semaphores */
	sem_destroy(&sem_read);

	return rs;
}

int get_index_menor(char** keys, int cant){
	char key_men[LEN_KEYVALUE];
	memset(key_men, 255, 255);

	int index = 0;
	int i;
	for(i=0;i<cant;i++){
		if (keys[i] != NULL)
			if (strcmp(keys[i], key_men) < 0) {
				//key_men = keys[i];
				strcpy(key_men, keys[i]);
				index = i;
			}
	}
	return index;
}


bool alguna_key_distinta_null(char** keys, int cant){
	bool rs = false;
	int i;
	for(i=0;i<cant;i++){
		if(keys[i] != NULL){
			rs = true;
			break;
		}
	}

	return rs;
}

bool file_reduce_es_de_red(t_files_reduce* fr) {
	return !nodo_es_local(fr->ip, fr->puerto);
}
bool file_reduce_es_local(t_files_reduce* fr) {
	return nodo_es_local(fr->ip, fr->puerto);
}
/*
int aplicar_reduce_local(t_list* files, char*script_reduce,
	char* filename_result) {
	int i = 0;
	int rs;
	int cant_files = list_size(files);
	t_list* local_files = list_create();
	void _convertir_a_absoluto(char* filename) {
		list_add(local_files, convertir_a_temp_path_filename(filename));
	}
	list_iterate(files, (void*) _convertir_a_absoluto);
	//creo un descriptor de archivo por cada file
	FILE** fd = malloc(cant_files * sizeof(FILE*));

	//abro los archivos
	i = 0;
	void _open_file(char* filename) {
		fd[i] = fopen(filename, "r");
		i++;
	}
	list_iterate(local_files, (void*) _open_file);

	//limpio local_files
	list_destroy_and_destroy_elements(local_files, (void*) free);

	int index_menor; //para guardar el menor item
	//creo una lista de key para guardar las key de cada file
	size_t len_key = LEN_KEYVALUE;
	char* key = NULL;
	char** keys = malloc(sizeof(char*) * cant_files);
	int _reduce_local() {
		int i = 0;

		//cargo las keys, con su primer valor
		for (i = 0; i < cant_files; i++) {
			//rs = getline(&(keys[i]), &len_key, fd[i]);
			key = malloc(LEN_KEYVALUE);
			len_key = LEN_KEYVALUE;
			rs = getline(&key, &len_key, fd[i]);
			if (rs != -1) {
				//key[rs] = '\0';
				keys[i] = key;
				//strcpy(keys[i], key);
			}
			//free(key);
		}
		//aca ya tengo todas las keys

		while (alguna_key_distinta_null(keys, cant_files)) {
			//obtengo cual es el menor
			index_menor = get_index_menor(keys, cant_files);
			//el menor lo mando a stdinn (keys[i])
			printf("%s\n", keys[index_menor]);
			write(pipes[PARENT_WRITE_PIPE][WRITE_FD] , keys[index_menor],
					strlen(keys[index_menor]) + 1);
			//leo el siguiente elmento del fd[index_menor]
			len_key = LEN_KEYVALUE;
			rs = getline(&(keys[index_menor]), &len_key, fd[index_menor]);
			//rs = getline(&key, &len_key, fd[index_menor]);

			//si es igual a -1, termino el file, marco como null la key para que la ignore cuando obtiene el menor
			if (rs == -1) {
				FREE_NULL(keys[index_menor]);
				keys[index_menor] = NULL;
				//keys[index_menor] = KEYVALUE_END;
			}

		}
		//si llego hasta aca termino de enviarle cosas por stdin,
		//cierro el stdin
		close(pipes[PARENT_WRITE_PIPE][WRITE_FD] );

		int count;
		char* new_file_reduced;
		new_file_reduced = convertir_a_temp_path_filename(filename_result);	//genero filename absoluto
		FILE* file_reduced = txt_open_for_append(new_file_reduced);	//creo el file
		FREE_NULL(new_file_reduced);		//limpio el nombre
		char* buffer = malloc(1024);//creo un buffer para ir almacenando el stdout
		i = 0;
		do {
			count = read(pipes[PARENT_READ_PIPE][READ_FD], buffer, 1024);
			fwrite(buffer, count, 1, file_reduced);
			if (count > 0) {
				buffer[count] = '\0';
				printf("%s\n", buffer);
			}

			i++;
		} while (count != 0);
		close(pipes[PARENT_READ_PIPE][READ_FD]);
		fclose(file_reduced);
		FREE_NULL(buffer);

////////////////////////////////////////
		for (i = 0; i < cant_files; i++)
			fclose(fd[i]);
		FREE_NULL(fd);

		for (i = 0; i < cant_files; i++) {
			free(keys[i]);
		}
		FREE_NULL(keys);

		return 0;
	}

	return (int) ejecutar_script(script_reduce, (void*) _reduce_local);
}
*/
bool nodo_es_local(char* ip, int puerto) {
	return string_equals_ignore_case(ip, NODO_IP()) && puerto == NODO_PORT();
}

char* convertir_a_temp_path_filename(char* filename) {
	pthread_mutex_lock(&mutex);
	char* new_path_file = string_new();
	string_append(&new_path_file, NODO_DIRTEMP());
	string_append(&new_path_file, "/");
	string_append(&new_path_file, filename);
	pthread_mutex_unlock(&mutex);
	return new_path_file;
}
/*
int thread_aplicar_map(int fd) {
	pthread_t th;

	pthread_create(&th, NULL, (void*) _aplicar_map, &fd);
	pthread_detach(th);

	return 0;
}
*/
/*
int _aplicar_map(void* param) {
	int fd = *((int *) param);

	//recibo el map que me envio
	t_map* map = NULL;
	map = recibir_mensaje_map(fd);
	char* filename_script = generar_nombre_map_tmp();
	recibir_mensaje_script_y_guardar(fd, filename_script);
	int rs;
	pthread_mutex_lock(&mutex);
	log_trace(logger, "Aplicando mapper %s sobre el bloque %d", filename_script, map->archivo_nodo_bloque->numero_bloque);
	pthread_mutex_unlock(&mutex);
	///////////////////////////////////////////////////////////////////////


	pthread_mutex_lock(&mutex);
	log_trace(logger, "_______________________socket %d", fd);
	pthread_mutex_unlock(&mutex);
	rs = aplicar_map(map->archivo_nodo_bloque->numero_bloque, filename_script,map->info->resultado);
	pthread_mutex_lock(&mutex);
	log_trace(logger, "Info fin thread: res: %d", rs);
	log_trace(logger,
			"*****************************************************************");
	pthread_mutex_unlock(&mutex);

	char buffer[32];
	if (recv(fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT) == 0) {
		printf("El  NODOOOO el sock %d conexionNNNNNNNNNNNNNNNNNNNNN \n", fd);
		// if recv returns zero, that means the connection has been closed:

		//close(fd);
		return -1;
		// do something else, e.g. go on vacation
	}

	t_msg* msg = argv_message(MAPPER_TERMINO, 0);

	rs = enviar_mensaje(fd, msg);
	if (rs < 0)
		perror("holaaaa");

	destroy_message(msg);


	pthread_mutex_lock(&mutex);
	log_trace(logger, "Enviado MAPPER_TERMINO al job");
	log_trace(logger, "Fin mapper guardado en %s", map->info->resultado);
	pthread_mutex_unlock(&mutex);

	//close(fd);
	free(filename_script);
	remove(filename_script);

	recibir_mensaje_nodo_ok(fd);

	return rs;
}
*/


sem_t* sem_crear(int* shmid, key_t* shmkey){
	sem_t* sem= NULL;
	//pid_t pid; /*      fork pid                */
	//unsigned int n; /*      fork count              */
	unsigned int value; /*      semaphore value         */

	/* initialize a shared variable in shared memory */

	pthread_mutex_lock(&mutex);
	char* sem_name = string_from_format("pSem_%d", contador_ftok);
	contador_ftok++;
	//shmkey = ftok("/dev/null", 5); /* valid directory name and a number */
	*shmkey = ftok("/dev/null", contador_ftok); /* valid directory name and a number */
	pthread_mutex_unlock(&mutex);



	printf("shmkey for p = %d\n", *shmkey);
	*shmid = shmget(*shmkey, sizeof(int), 0644 | IPC_CREAT);
	if (*shmid < 0) { /* shared memory error check */
		perror("shmget\n");
		exit(1);
	}

	///////////////////////////////////////////
	value = 0;
	/* initialize semaphores for shared processes */
	//sem = sem_open("pSem", O_CREAT | O_EXCL, 0644, value);


	//sem = sem_open("pSem", O_CREAT , 0644, value);
	sem = sem_open(sem_name, O_CREAT , 0644, value);
	if(sem==SEM_FAILED){
		perror("sem_open___");
		printf("***************************************************sdfadfasd\n");
		exit(1);
	}

	/* name of semaphore is "pSem", semaphore is reached using this name */
	//sem_unlink("pSem");
	sem_unlink(sem_name);
	/* unlink prevents the semaphore existing forever */
	/* if a crash occurs during the execution         */
	printf ("semaphores initialized.\n\n");

	FREE_NULL(sem_name);

	return sem;
}

int aplicar_map_ok(int n_bloque, char* script_map, char* filename_result, pthread_mutex_t* mutex){
	int rs;
	int _reader_writer(int fdreader, int fdwriter) {
		int _writer(int fd) {
			int rs = 0;
			// Write to child’s stdin
			char* stdinn = getBloque(n_bloque);
			size_t len = strlen(stdinn);
			if (stdinn[len - 1] != '\n') {
				len += 1;
				stdinn = realloc(stdinn, len);
				stdinn[len] = '\0';
				stdinn[len - 1] = '\n';
			}
			pthread_mutex_lock(mutex);
			log_trace(logger, "Escribiendo en stdin bloque %d", n_bloque);
			pthread_mutex_unlock(mutex);

			size_t bytes_escritos = 0, bytes_leidos = 0, i=0;
			//inicializo en el primer enter
			size_t len_buff_write = 0;
			//size_t len_buff_read = LEN_KEYVALUE;

			do {
				len_buff_write = len_hasta_enter(stdinn + bytes_leidos);
				bytes_escritos = escribir_todo(fd, stdinn + bytes_leidos, len_buff_write);
				if(bytes_escritos<0){
					rs = -1;
					break;
				}
				/*pthread_mutex_lock(&mx_mr);
				aux = 0;
				bytes_escritos = 0;
				do {
					aux = write(pipes[PARENT_WRITE_PIPE][WRITE_FD],	stdinn + bytes_leidos + bytes_escritos,	len_buff_write - bytes_escritos);
					bytes_escritos = bytes_escritos + aux;
				} while (bytes_escritos < len_buff_write);
				fsync(pipes[PARENT_WRITE_PIPE][WRITE_FD]);
				//fprintf(stdout, "*************total: %d\n", bytes_escritos);
				pthread_mutex_unlock(&mx_mr);*/

				len = len - bytes_escritos;
				bytes_leidos = bytes_leidos + len_buff_write;
				//fprintf(stdout, "bytes escritos: %d\n", bytes_escritos);
				//printf("contador %d bloque %d\n", i, n_bloque);
				i++;

			} while (len > 0);

			if (rs < 0) {
				pthread_mutex_lock(mutex);
				log_trace(logger, "Error al escribir en stdin");
				pthread_mutex_unlock(mutex);
				exit(-1);
				//return -1;
			}

			pthread_mutex_lock(mutex);
			log_trace(logger, "Fin escritura stdin bloque %d", n_bloque);
			pthread_mutex_unlock(mutex);
			FREE_NULL(stdinn);


			printf("antes del close\n");
			//rs = close(pipes[PARENT_WRITE_PIPE][WRITE_FD]);
			rs = close(fd);
			if (rs < 0) {
				perror("________________________---close");
				exit(-1);
				//return -1;
			}
			printf("despues del close\n");

			/*FILE* file = fopen(param_ordenar->origen, "r");
			char* linea = NULL;
			linea = malloc(LEN_KEYVALUE);
			size_t len_linea = LEN_KEYVALUE;
			int rs;
			while ((rs = getline(&linea, &len_linea, file)) > 0) {
				escribir_todo(fd, linea, strlen(linea));
			}
			free(linea);
			fclose(file);
			close(fd);*/
			return 0;
		}

		//lanzo hilo writer stdin;
		pthread_t th_writer, th_reader;
		pthread_create(&th_writer, NULL, (void*) _writer, (void*) fdwriter);

		t_reader treader;
		treader.fd = fdreader;
		char* new_file_map_disorder = convertir_a_temp_path_filename(filename_result);
		string_append(&new_file_map_disorder, "-disorder.tmp");
		treader.destino = new_file_map_disorder ;

		pthread_create(&th_reader, NULL, (void*) reader_and_save_as, (void*) &treader);

		//lockeo hasta que termine de escribir y leer el stdin y stdout
		pthread_join(th_writer, NULL);
		pthread_join(th_reader, NULL);

		//hasta aca ya tengo el map del archivo
		//me falta ordenarlo


		pthread_mutex_lock(mutex);
		log_trace(logger, "Fin lectura stdout, guardado en archivo %s",	new_file_map_disorder);
		log_trace(logger, "Ordenando archivo %s", new_file_map_disorder);
		pthread_mutex_unlock(mutex);

		char* result_order = string_from_format("%s/%s", NODO_DIRTEMP(), filename_result);
		ordenar_map(new_file_map_disorder, result_order, mutex);
		FREE_NULL(result_order);

		pthread_mutex_lock(mutex);
		log_trace(logger, "Fin orden, generado archivo final > %s",	filename_result);
		pthread_mutex_unlock(mutex);

		puts("map Fin hilo lectura stdout\n");

		FREE_NULL(new_file_map_disorder);

		return 0;
	}
	char* nombre_proc = string_from_format("map-on-block:%d", n_bloque);
	rs = ejecutar_script(script_map, nombre_proc, _reader_writer, mutex);
	FREE_NULL(nombre_proc);
	//rs = ejecutar_script("/home/utnso/Escritorio/tests/mapper.sh", "mapperR", _reader_writer);

	return rs;
}

int aplicar_map_final(int n_bloque, char* script_map, char* filename_result){
	key_t shmkey; /*      shared memory key       */
	int shmid; /*      shared memory id        */
	sem_t *sem; /*      synch semaphore         *//*shared */
	sem = sem_crear(&shmid, &shmkey);

	//key_t shmkey_read; /*      shared memory key       */
	//int shmid_read; /*      shared memory id        */
	//sem_t *sem_read; /*      synch semaphore         *//*shared */
	//sem_read = sem_crear(&shmid_read, &shmkey_read);
	sem_t sem_read;
	sem_init(&sem_read, 0, 0);

	///////////////////////////////////
	int aux;
	pthread_mutex_t mx_mr;/* mutex para que no haya deadlock por el fork*/
	pthread_mutex_init(&mx_mr, NULL);
	int pipes[NUM_PIPES][2];

	int _ejecutar_script(char* script, int (*procesar_std)()) {
		// pipes for parent to write and read

		if (pipe(pipes[PARENT_READ_PIPE]) == -1)
			handle_error("pipe");

		if ((pipe(pipes[PARENT_WRITE_PIPE])) == -1)
			handle_error("pipe");

		///////////////////////////////////////////

		int p = fork();
		if (p < 0)
			handle_error("fork pipe stdin stdout");

		if (p == 0) {
			//hijo
			/*pthread_mutex_t mutex;
			pthread_mutex_init(&mutex, NULL);
			pthread_mutex_lock(&mutex);*/
			char *argv[] = { script, NULL };
			//fflush(stdin);
			if (dup2(pipes[PARENT_WRITE_PIPE][READ_FD], STDIN_FILENO) < 0) {
				perror("dup2 STDIN_FILENO");
				exit(0);
			}
			//fflush(stdout);
			if (dup2(pipes[PARENT_READ_PIPE][WRITE_FD], STDOUT_FILENO) < 0) {
				perror("dup2 STDIN_FILENO");
				exit(-1);
			}

			close(pipes[PARENT_WRITE_PIPE][READ_FD]);
			close(pipes[PARENT_READ_PIPE][WRITE_FD]);
			close(pipes[PARENT_READ_PIPE][READ_FD]);
			close(pipes[PARENT_WRITE_PIPE][WRITE_FD]);

			/*pthread_mutex_unlock(&mutex);
			pthread_mutex_destroy(&mutex);*/
			sem_post(sem);
			execv(argv[0], argv);

			perror("Errro execv");
			printf("argv[0]:%s\n", argv[0]);
			exit(-1);

			return 0;
		} else {
			//waitpid(p, NULL, NULL);
			printf("*************************************ANtes wait\n");
			sem_wait(sem);
			printf("*************************************Despues wait\n");

			close(pipes[PARENT_WRITE_PIPE][READ_FD]);
			close(pipes[PARENT_READ_PIPE][WRITE_FD]);

			sem_post(&sem_read);
			int rs ;
			rs =  procesar_std();

			//pthread_t th_stdin;
			//pthread_create(&th_stdin, NULL, (void*)procesar_std(), NULL);
			//pthread_join(th_stdin, (void*)&rs);
			//int status=0;
			//wait(&status);

			puts("listo");

			//wait(0);
			return rs;

		}
		return 0;
	}
	/*
	 * funcion map
	 */
	int __aplicar_map() {
		int res = 0;
		//int count;

		// Write to child’s stdin
		char* stdinn = getBloque(n_bloque);

		size_t len = strlen(stdinn);
		if (stdinn[len - 1] != '\n') {
			len += 1;
			stdinn = realloc(stdinn, len);
			stdinn[len] = '\0';
			//printf("%s\n", stdinn);
			stdinn[len - 1] = '\n';
		}

		pthread_mutex_lock(&mx_mr);
		log_trace(logger, "Escribiendo en stdin bloque %d", n_bloque);
		pthread_mutex_unlock(&mx_mr);
		// Read from child’s stdout

		//FILE* file_disorder = txt_open_for_append_closeonfork(new_file_map_disorder);



		size_t bytes_escritos = 0, bytes_leidos = 0;
		size_t i = 0;

		//inicializo en el primer enter
		size_t len_buff_write = 0;
		//size_t len_buff_read = LEN_KEYVALUE;

		if (len < len_buff_write)
			len_buff_write = len;


		do {
			len_buff_write = len_hasta_enter(stdinn + bytes_leidos);

			pthread_mutex_lock(&mx_mr);
			aux = 0;
			bytes_escritos = 0;
			do{
				aux= write(pipes[PARENT_WRITE_PIPE][WRITE_FD] , stdinn + bytes_leidos+bytes_escritos, len_buff_write - bytes_escritos);
				//fprintf(stdout, "bytesEscritos: %d\n", aux);
				if(aux<0){
					printf("_____________write Error\n");
					perror("write:::::::::::::::::::::::");
					exit(-1);
				}
				bytes_escritos= bytes_escritos + aux;
			}while(bytes_escritos<len_buff_write);
			fsync(pipes[PARENT_WRITE_PIPE][WRITE_FD]);
			//fprintf(stdout, "*************total: %d\n", bytes_escritos);
			pthread_mutex_unlock(&mx_mr);
/*
			if (bytes_escritos == -1) {
				perror("writeeeeee");
				return -1;
			}*/
			len = len - bytes_escritos;
			bytes_leidos = bytes_leidos + len_buff_write;
			//fprintf(stdout, "bytes escritos: %d\n", bytes_escritos);
			//printf("contador %d bloque %d\n", i, n_bloque);
			i++;

		} while (len > 0);

		if (res < 0) {
			pthread_mutex_lock(&mx_mr);
			log_trace(logger, "Error al escribir en stdin");
			pthread_mutex_unlock(&mx_mr);
			return -1;
		}


		pthread_mutex_lock(&mx_mr);
		log_trace(logger, "Fin escritura stdin bloque %d", n_bloque);
		pthread_mutex_unlock(&mx_mr);
		FREE_NULL(stdinn);


		fprintf(stdout, "antes del close\n");
		int rs;
		rs = close(pipes[PARENT_WRITE_PIPE][WRITE_FD] );
		if(rs<0){
			perror("________________________---close");
			exit(-1);
		}
		printf("despues del close\n");

		puts("_________antes waittttttttttttttttt");
		sem_wait(&sem_read);
		puts("_______despues waittttttttttttttttt");

		return res;
	}

	pthread_t pth_read;
	int _read() {
		int rs;
		printf("antes sem_wait(sem_read)\n");
		rs = sem_wait(&sem_read);
		if(rs<0){
			perror("sem_waittttttttttt");
			exit(-1);
		}
		printf("Despues (sem_read)\n");
		char* new_file_map_disorder = convertir_a_temp_path_filename(filename_result);
		string_append(&new_file_map_disorder, "-disorder.tmp");
		FILE* file_disorder = txt_open_for_append(new_file_map_disorder);
		if(file_disorder == NULL){
			printf("txt_open_for_append(new_file_map_disorder)\n");
			perror("_____________opennn");
			exit(-1);
		}
		char buffer[LEN_KEYVALUE];
		memset(buffer, 0, LEN_KEYVALUE);

		int count;
		pthread_mutex_lock(&mx_mr);
		log_trace(logger, "Empezando a leer stdout y guardar en archivo %s", new_file_map_disorder);
		pthread_mutex_unlock(&mx_mr);


		/*while ((count = read(pipes[PARENT_READ_PIPE][READ_FD], buffer,LEN_KEYVALUE)) > 0) {
			fwrite(buffer, count, 1, file_disorder);
			//fprintf(stdout, "bytes leidos>>>> %d buffer: %s\n", count, buffer);
			memset(buffer, 0, LEN_KEYVALUE);
		}*/
		//int rs ;
		while ((count = read(pipes[PARENT_READ_PIPE][READ_FD], buffer,1)) > 0) {

			rs = fwrite(buffer, 1, 1, file_disorder);
			if(rs<=0){
				perror("_________fwrite");
				exit(-1);
			}

			//fprintf(stdout, "bytes leidos>>>> %d buffer: %s\n", count, buffer);
			//memset(buffer, 0, LEN_KEYVALUE);
		}
		puts("antes __________readddddddddd");
		sem_post(&sem_read);
		puts("despues_______readddddddddd");

		if (count == -1) {
			perror("map readddd");
		}
		printf("antes close(file_disorder)\n");
		rs = fclose(file_disorder);
		if(rs<0){
			perror("_______fclose");
			exit(-1);
		}

		printf("despues close(file_disorder)\n");

		pthread_mutex_lock(&mx_mr);
		log_trace(logger, "Fin lectura stdout, guardado en archivo %s",	new_file_map_disorder);
		log_trace(logger, "Ordenando archivo %s", new_file_map_disorder);
		pthread_mutex_unlock(&mx_mr);

		ordenar_y_guardar_en_temp(new_file_map_disorder, filename_result);//guardo el file definitivo en el tmp
		FREE_NULL(new_file_map_disorder);
		pthread_mutex_lock(&mx_mr);
		log_trace(logger, "Fin orden, generado archivo final > %s",	filename_result);
		pthread_mutex_unlock(&mx_mr);


		puts("map Fin hilo lectura stdout\n");
		return 0;
	}

	pthread_create(&pth_read, NULL, (void*) _read, NULL);
	int rs ;
	pthread_t pth_write;
	void _writemap(){
		rs =  _ejecutar_script(script_map, (void*) __aplicar_map);
	}
	pthread_create(&pth_write, NULL, (void*)_writemap, NULL);
	pthread_join(pth_write, NULL);
	pthread_join(pth_read, NULL);


	rs = close(pipes[PARENT_READ_PIPE][READ_FD]);
	if(rs<0){
		perror("____________________closepipe_readread");
	}

	pthread_mutex_destroy(&mx_mr);

	/* shared memory detach */
	shmctl(shmid, IPC_RMID, 0);
	/* cleanup semaphores */
	sem_destroy (sem);

	sem_destroy(&sem_read);

	return rs;
}

int ordenar_map(char* origen, char* destino, pthread_mutex_t* mutex){
	t_ordenar* p_ordenar;
	p_ordenar = malloc(sizeof(t_ordenar));
	p_ordenar->origen = origen;
	p_ordenar->destino =destino;
	p_ordenar->mutex = mutex;
	pthread_t th_ordenar;
	pthread_create(&th_ordenar, NULL, (void*)ordenar, (void*)p_ordenar);
	pthread_join(th_ordenar, NULL);

	pthread_mutex_lock(mutex);
	log_trace(logger, "Fin ordenar %s, res: %s", origen, destino);
	pthread_mutex_unlock(mutex);

	return 0;
}

int ordenar_y_guardar_en_temp(char* file_desordenado, char* destino) {

	pthread_mutex_lock(&mutex);
	char* commando_ordenar = string_new();
	/*string_append(&commando_ordenar, "cat ");
	string_append(&commando_ordenar, file_desordenado);
	string_append(&commando_ordenar, " | /usr/bin/sort > ");*/
	string_append(&commando_ordenar, "/usr/bin/sort ");
	string_append(&commando_ordenar, file_desordenado);
	string_append(&commando_ordenar, " > ");
	string_append(&commando_ordenar, NODO_DIRTEMP());
	string_append(&commando_ordenar, "/");
	string_append(&commando_ordenar, destino);

	//printf("%s\n", commando_ordenar);
	log_trace(logger, "Empezando a ordenar archivo: %s", commando_ordenar);

	int rs;
	rs = system(commando_ordenar);
	perror("_________--system");
	printf("resultado:::::::::::::::::::::::::%d\n", rs);


	log_trace(logger, "Fin Comando ordenar");
	pthread_mutex_unlock(&mutex);
	FREE_NULL(commando_ordenar);

	//borro el file
	remove(file_desordenado);
	return 0;
}

int grabar_en_temp(char* filename, char* data) {

	write_file(filename, data, strlen(data));
	printf("archivo temporal creado: %s\n", filename);

	return 0;

	/*
	 char* nombre_nuevo_archivo = NULL;
	 nombre_nuevo_archivo = file_combine(NODO_DIRTEMP(), filename);
	 char* timenow = temporal_get_string_time();
	 string_append(&nombre_nuevo_archivo, timenow);
	 FREE_NULL(timenow);

	 write_file(nombre_nuevo_archivo, data, strlen(data));

	 printf("archivo creado: %s\n", nombre_nuevo_archivo);
	 //free_null(&nombre_nuevo_archivo);
	 return nombre_nuevo_archivo;
	 */
}

void agregar_cwd(char* file) {
	char cwd[1024];
	if (getcwd(cwd, sizeof(cwd)) == NULL)
		handle_error("getcwd() error");

	char* aux = malloc(strlen(file) + 1);
	strcpy(aux, file);
	strcpy(file, cwd);
	strcat(file, aux);
	free(aux);
}

void iniciar_server_thread() {
	pthread_t th;

	//pthread_join(th, NULL);
	pthread_create(&th, NULL, (void*) iniciar_server, NULL);

}

char* generar_nombre_script() {
	char* timenow = temporal_get_string_time();
	char* file_map1 = string_new();
	string_append(&file_map1, "job_script_map_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, ".sh");
	free(timenow);
	return file_map1;
}

char* generar_nombre_map_tmp(t_mapreduce* mapreduce) {
	char* timenow = temporal_get_string_time();

	/*char* file_map1 = string_new();
	string_append(&file_map1, "job_script_map_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, ".sh");
	*/
	char* file_map1 = NULL;
	file_map1 = string_from_format("script_job_%d_map_%d_%s.sh", mapreduce->job_id, mapreduce->id, timenow);


	char* tmp;
	tmp = convertir_a_temp_path_filename(file_map1);
	free(file_map1);
	free(timenow);

	return tmp;
}

char* generar_nombre_reduce_tmp(t_mapreduce* mapreduce) {
	char* timenow = temporal_get_string_time();

	/*char* file_map1 = string_new();
	string_append(&file_map1, "job_script_reduce_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, ".sh");*/
	char* file_map1 = NULL;
	file_map1 = string_from_format("script_job_%d_reduce_%d_%s.sh", mapreduce->job_id, mapreduce->id, timenow);

	char* tmp;
	tmp = convertir_a_temp_path_filename(file_map1);
	free(file_map1);
	free(timenow);

	return tmp;
}

int aplicar_reduce(t_reduce* reduce, char* script) {
	t_list* files_to_reduce = list_create();
	t_files_reduce* file_reduce1 = NULL;
	void _crear_files_reduce(t_nodo_archivo* na) {
		//creo un archivo para reducir
		file_reduce1 = NULL;
		file_reduce1 = malloc(sizeof *file_reduce1);
		strcpy(file_reduce1->archivo, na->archivo);
		strcpy(file_reduce1->ip, na->nodo_base->red.ip);
		file_reduce1->puerto = na->nodo_base->red.puerto;
		list_add(files_to_reduce, file_reduce1);
	}
	list_iterate(reduce->nodos_archivo, (void*) _crear_files_reduce);



	//aplicar_reduce_local_red(files_to_reduce, script, reduce->info->resultado);
	aplicar_reduce_ok(files_to_reduce, script, reduce->info->resultado, &mutex);


	pthread_mutex_lock(&mutex);
	log_trace(logger, "Guardado en %s\n", reduce->info->resultado);
	pthread_mutex_unlock(&mutex);

	list_destroy_and_destroy_elements(files_to_reduce, free);

	return 0;
}

int procesar_mensaje(int fd, t_msg* msg) {
	char* bloque;
	char* filename_script;
	int n_bloque = 0, rs;
	//char* buff;
	char* file_data;
	t_map* map = NULL;
	t_reduce* reduce = NULL;
	switch (msg->header.id) {
	case JOB_REDUCER:
		destroy_message(msg);
		pthread_mutex_lock(&mutex);
		log_trace(logger, "******************************************************");
		log_trace(logger, "Recibido nuevo reducer");
		pthread_mutex_unlock(&mutex);
		//recibo el reduce que me envio
		reduce = NULL;
		reduce = recibir_mensaje_reduce(fd);
		filename_script = generar_nombre_reduce_tmp(reduce->info);
		recibir_mensaje_script_y_guardar(fd, filename_script);

		///////////////////////////////////////////////
		pthread_mutex_lock(&mutex);
		log_trace(logger, "Aplicando reducer %s sobre %d archivos",	filename_script, list_size(reduce->nodos_archivo));
		pthread_mutex_unlock(&mutex);

		//aplicar_reduce(reduce, filename_script);
		aplicar_reduce(reduce, filename_script);

		pthread_mutex_lock(&mutex);
		log_trace(logger, "Fin reducer guardado en %s",	reduce->info->resultado);
		pthread_mutex_unlock(&mutex);
		remove(filename_script);
		free(filename_script);



		reduce_free(reduce);

		msg = argv_message(REDUCER_TERMINO, 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		printf("_DDDDDDDDDD_Enviado REDUCER_TERMINO al job");
		break;
	case JOB_MAPPER:

		destroy_message(msg);
		pthread_mutex_lock(&mutex);
		log_trace(logger, "******************************************************");

		pthread_mutex_unlock(&mutex);
		//recibo el map que me envio
		map = NULL;
		map = recibir_mensaje_map(fd);
		log_trace(logger, "Recibido nuevo mapper %d socck %d", map->info->id, fd);
		filename_script = generar_nombre_map_tmp(map->info);
		recibir_mensaje_script_y_guardar(fd, filename_script);
		///////////////////////////////////////////////////////////////////////
		pthread_mutex_lock(&mutex);
		log_trace(logger, "Aplicando mapper %s sobre el bloque %d sock %d",	filename_script, map->archivo_nodo_bloque->numero_bloque, fd);
		pthread_mutex_unlock(&mutex);

		//pthread_mutex_lock(&mx_mr);
		//rs = aplicar_map_final(map->archivo_nodo_bloque->numero_bloque, filename_script, map->info->resultado);
		rs = aplicar_map_ok(map->archivo_nodo_bloque->numero_bloque, filename_script, map->info->resultado, &mutex);

		if(rs<0)
			printf("errrrrrrrrrrrrrrror map %d\n", map->archivo_nodo_bloque->numero_bloque);
		//pthread_mutex_unlock(&mx_mr);

		pthread_mutex_lock(&mutex);
		log_trace(logger, "Fin mapper %d sock %d guardado en %s", map->info->id, fd, map->info->resultado);
		pthread_mutex_unlock(&mutex);
		remove(filename_script);
		free(filename_script);


		msg = argv_message(MAPPER_TERMINO, 0);

		char buffer[32];
		if (recv(fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT) == 0) {
			printf("El  JOBBBBB map %d el sock %d conexion cerrada??????????????????? \n", map->info->id,	fd);
			// if recv returns zero, that means the connection has been closed:

			//close(fd);
			break;
			// do something else, e.g. go on vacation
		}else
			printf("map %d, sock %d activoOOO\n",map->info->id, fd);


		rs = enviar_mensaje(fd, msg);
		printf("Termino map %d sock %d\n", map->info->id, fd);
		pthread_mutex_lock(&mutex);
		log_trace(logger, "Enviado MAPPER_TERMINO id %d al job sock %d", map->info->id, fd);
		pthread_mutex_unlock(&mutex);
		destroy_message(msg);

		//recibir_mensaje_nodo_ok(fd);
		printf("fin mapppppppppppppppp %d socket %d\n", map->info->id, fd);

		map_free_all(map);


		break;
	case FS_AGREGO_NODO:
		log_trace(logger, "El nodo se agrego al fs con id %d", msg->argv[0]);
		destroy_message(msg);

		//close(fd);
		break;
	case NODO_GET_FILECONTENT_DATA:
		//lo convierto a path absoluto
		//buff = convertir_a_temp_path_filename(msg->stream);
		//obtengo el char
		file_data = getFileContent(msg->stream);
		//FREE_NULL(buff);

		//envio el archivo
		destroy_message(msg);
		//msg = string_message(NODO_GET_FILECONTENT, file, 0);
		enviar_mensaje_sin_header(fd, strlen(file_data) + 1, file_data);

		/*if((rs = enviar_mensaje(fd, msg))<0){
		 printf("Error al arhivo temp NODO_GET_FILECONTENT\n");
		 perror("enviar_mensaje ");
		 }*/
		//destroy_message(msg);
		break;
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
		printf("CloseSSSSSSSS\n");
		//close(fd);

		break;
	case FS_GRABAR_BLOQUE:

		bloque = malloc(TAMANIO_BLOQUE_B);
		memcpy(bloque, msg->stream, msg->argv[1]);	//1 es el tamaño real
		pthread_mutex_lock(&mutex);
		memset(bloque + msg->argv[1], '\0', TAMANIO_BLOQUE_B - msg->argv[1]);
		setBloque(msg->argv[0], bloque);
		pthread_mutex_unlock(&mutex);
		FREE_NULL(bloque);

		destroy_message(msg);

		enviar_mensaje_nodo_ok(fd);

		break;
	case FS_HOLA:

		destroy_message(msg);
		msg = argv_message(NODO_HOLA, 0);
		enviar_mensaje(fd, msg);
		destroy_message(msg);

		break;
	default:
		break;
	}
	return 0;
}

void iniciar_server() {
	log_trace(logger, "Iniciado server. Puerto: %d\n", NODO_PORT());

	server_socket_select(NODO_PORT(), (void*) procesar_mensaje);
}

void iniciar_server_fork(){
	log_trace(logger, "Iniciado server. Puerto: %d\n", NODO_PORT());
	int listener = server_socket(NODO_PORT());
	if (listener < 0) {
		printf("ERRROr listener %d\n", NODO_PORT());
		return;
	}

	int nuevaConexion;
	int pfork = 1;
	t_msg* msg =  NULL;
	signal(SIGCHLD, SIG_IGN);
	while (pfork) {

		nuevaConexion = accept_connection(listener);
		if (nuevaConexion < 0){
			perror("accept");
			return;
		}
		log_trace(logger, "******************NuevaConexion sock %d\n", nuevaConexion);

		 msg = recibir_mensaje(nuevaConexion);
		if(msg!=NULL){

			if(msg->header.id==JOB_REDUCER || msg->header.id == JOB_MAPPER ){
				procesar_mensaje(nuevaConexion, msg);
			}else{

				pfork = fork();
			}
		}
	}
	if(pfork==0){
		//hijo
		printf("soy el hijo del fork;\n");
		atenderProceso_fork(nuevaConexion, msg );
		printf("exit(0) sock %d\n", nuevaConexion);
		//exit(0);
		//_exit(0);
	}
}

void* atenderProceso_fork(int fd, t_msg* msg){


	printf("nuevo fork sock %d\n", fd);

	if(msg == NULL){
		perror("recibir_msgggg");
		printf("reccc msj %d\n", fd);
		return NULL;
	}


	//pthread_mutex_lock(&mutex);
	int rs = procesar_mensaje(fd, msg);
	if(rs != 0){
		printf("ERRRRRRORRRRRRRRRR\n");
	}
	//pthread_mutex_unlock(&mutex);
	printf("Fin fork sock %d\n", fd);
	close(fd);

	return NULL;
}

bool requiere_hilo(t_msg* msg){
	return msg->header.id == FS_GRABAR_BLOQUE
			|| msg->header.id == NODO_GET_FILECONTENT_DATA
			|| msg->header.id == NODO_GET_BLOQUE
			|| msg->header.id == JOB_MAPPER
			|| msg->header.id == JOB_REDUCER;
}

void incicar_server_sin_select() {

	log_trace(logger, "Iniciado server. Puerto: %d\n",NODO_PORT() );
	pthread_t thread;
	int listener = server_socket(NODO_PORT());
	if(listener<0){
		printf("ERRROr listener %d\n", NODO_PORT());
		return ;
	}
	int nuevaConexion;
	while (true) {

		nuevaConexion = accept_connection(listener);
		if(nuevaConexion<0)
			perror("accept");
		log_trace(logger, "******************NuevaConexion sock %d\n", nuevaConexion);

		t_socket_msg* smsg = malloc(sizeof(t_socket_msg));
		(*smsg).socket = nuevaConexion;

		//recibo el mensaje para saber si es algo para lanzar hilo
		smsg->msg = recibir_mensaje(smsg->socket);
		if (smsg->msg == NULL) {
			perror("recibir_msgggg");
			printf("reccc msj %d\n", smsg->socket);
			//return NULL;
		}
		/*
		 * PARA SALIR DEL NODO
		if(smsg->msg->header.id==123){
			close(nuevaConexion);
			destroy_message(smsg->msg);
			FREE_NULL(smsg);
			return;
		}*/

		if(requiere_hilo(smsg->msg)){
			printf("sock %d nuevo hilo\n", smsg->socket);
			if(	(pthread_create(&thread, NULL, (void*)atenderProceso, smsg)) <0){
				perror("pthread_create");
			}else
				printf("genero nuevo thread el sock%d\n", smsg->socket);
			pthread_detach(thread);
		}else{
			//si no requiere hilo
			procesar_mensaje(smsg->socket, smsg->msg);
			close(smsg->socket);
			FREE_NULL(smsg);
		}




	}

}

void* atenderProceso(t_socket_msg* smsg){


	int fd = smsg->socket;

	printf("NuevoThread sock %d\n", fd);

	//pthread_mutex_lock(&mutex);
	int rs = procesar_mensaje(fd, smsg->msg);
	if(rs != 0){
		printf("ERRRRRRORRRRRRRRRR\n");
	}
	//pthread_mutex_unlock(&mutex);
	printf("FinThread sock %d\n", fd);
	close(fd);
	FREE_NULL(smsg);


	return NULL;
}

int NODO_CANT_BLOQUES() {
	return CANT_BLOQUES;
}

void probar_conexion_fs() {

	log_trace(logger, "Conectado al FS ... %s:%d", NODO_IP_FS(), NODO_PORT_FS());

	int fs;

	if ((fs = client_socket(NODO_IP_FS(), NODO_PORT_FS())) > 0) {
		log_trace(logger, "Conectado");
		//t_msg* msg= id_message(NODO_CONECTAR_CON_FS);
		t_msg* msg = NULL;
		msg = argv_message(NODO_CONECTAR_CON_FS, 0);
		if (enviar_mensaje(fs, msg) < 0) {
			log_trace(logger, "Error al enviar mensaje al FS");
			exit(EXIT_FAILURE);
		}
		destroy_message(msg);

		if ((msg = recibir_mensaje(fs)) == NULL) {
			log_trace(logger, "Error al recibir mensaje del FS");
			exit(EXIT_FAILURE);
		}

		if (msg->header.id == FS_NODO_QUIEN_SOS) {
			destroy_message(msg);

			msg = string_message(RTA_FS_NODO_QUIEN_SOS, NODO_IP(), 4,
					NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES(), NODO_ID());
			if ((enviar_mensaje(fs, msg)) < 0) {
				printf("No se pudo responder a %s",
						id_string(FS_NODO_QUIEN_SOS));
				exit(EXIT_FAILURE);
			}
			destroy_message(msg);

			log_trace(logger, "Info enviada al FS");

		} else
			log_trace(logger, "No se pudo conectar con el fs");

		//close(fs);
		log_trace(logger, "Conectado con fs en %s:%d\n", NODO_IP_FS(),NODO_PORT_FS());

		log_trace(logger,
				"Id: %d, %s:%d, Cant_bloques: %d, bin: %s, dirtmp: %s, nuevo:%d",
				NODO_ID(), NODO_IP(), NODO_PORT(), NODO_CANT_BLOQUES(),
				NODO_ARCHIVOBIN(), NODO_DIRTEMP(), NODO_NUEVO());
	} else {
		log_trace(logger, "No se pudo iniciar la escucha con el FS");
	}
}
void finalizar() {
	log_destroy(logger);
	config_destroy(config);
	data_destroy();



	printf("fin ok\n");
	//while (true);
}

void inicializar() {

	config = config_create(FILE_CONFIG);
	logger = log_create(FILE_LOG, "Nodo", true, LOG_LEVEL_TRACE);
	/*
	 char*f;	f = convertir_path_absoluto(FILE_CONFIG);
	 config = config_create(f);
	 free(f);

	 f = convertir_path_absoluto(FILE_LOG);
	 logger = log_create(f, "Nodo", true, LOG_LEVEL_INFO);
	 free(f);
	 */

	pthread_mutex_init(&mutex, NULL);

	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_init(&mx_data, NULL);


	_data = data_get(NODO_ARCHIVOBIN());
}

/*
 * devuelve un puntero con el archivo mapeado
 */
char* getFileContent(char* filename) {
	log_info(logger, "Inicio getFileContent(%s)", filename);
	char* content = NULL;

	//creo el espacio para almacenar el archivo
	char* path = file_combine(NODO_DIRTEMP(), filename);
	size_t size = file_get_size(path) + 1;
	content = malloc(size);
	printf("size: %d\n", size);
	char* mapped = NULL;
	mapped = file_get_mapped(path);
	memcpy(content, mapped, size);	//
	file_mmap_free(mapped, path);

	FREE_NULL(path);

	log_info(logger, "Fin getFileContent(%s)", filename);
	return content;

}

void setBloque(int32_t numero, char* bloquedatos) {

	pthread_mutex_lock(&mx_data);
	log_info(logger, "Inicio setBloque(%d)", numero);

	memset(_data + (numero * TAMANIO_BLOQUE_B), 0, TAMANIO_BLOQUE_B);
	memcpy(_data + (numero * TAMANIO_BLOQUE_B), bloquedatos, TAMANIO_BLOQUE_B);

	log_info(logger, "Fin setBloque(%d)", numero);
	pthread_mutex_unlock(&mx_data);
}
/*
 * devuelve una copia del bloque, hacer free
 */
char* getBloque(int32_t numero) {
	pthread_mutex_lock(&mutex);
	log_info(logger, "Ini getBloque(%d)", numero);
	pthread_mutex_unlock(&mutex);
	char* bloque = NULL;
	bloque = malloc(TAMANIO_BLOQUE_B);

	if(bloque==NULL){
		printf("ERRORR bloque %d\n", numero);
		perror("malloc");
		exit(1);
	}
	pthread_mutex_lock(&mx_data);
	//pthread_mutex_lock(&mutex);
	memcpy(bloque, &(_data[numero * TAMANIO_BLOQUE_B]), TAMANIO_BLOQUE_B);
	//pthread_mutex_unlock(&mutex);
	pthread_mutex_unlock(&mx_data);

	//memcpy(bloque, _bloques[numero], TAMANIO_BLOQUE);
	pthread_mutex_lock(&mutex);
	log_info(logger, "Fin getBloque(%d)", numero);
	pthread_mutex_unlock(&mutex);
	return bloque;
}

//devuelvo el archivo data.bin mappeado
void* data_get(char* filename) {

	if (!file_exists(filename)) {
		TAMANIO_DATA = 1024 * 1024 * NODO_TAMANIO_DATA_DEFAULT_MB(); //100MB
		char CREAR_DATA[1024];
		sprintf(CREAR_DATA, "truncate -s %dM %s", NODO_TAMANIO_DATA_DEFAULT_MB(), filename);

		printf("%s\n", CREAR_DATA);
		system(CREAR_DATA);
		/*
		FILE* file = NULL;
		file = fopen(filename, "w+");
		if (file == NULL) {
			handle_error("fopen");
		}

		log_trace(logger, "creando archivo de %d bytes ...\n", TAMANIO_DATA);
		//lo creo con el tamaño maximo
		void* dump = NULL;
		dump = malloc(TAMANIO_DATA);

		////grabo 0 en todo el nodo.
		memset(dump, 0, TAMANIO_DATA);
		fwrite(dump, TAMANIO_DATA, 1, file);
		FREE_NULL(dump);

		fclose(file);
		*/
	}
	//calculo la cantidad de bloques
	TAMANIO_DATA = file_get_size(filename);
	CANT_BLOQUES = TAMANIO_DATA / TAMANIO_BLOQUE_B;

	log_trace(logger, "Cant-bloques de %dmb: %d", TAMANIO_BLOQUE_MB, CANT_BLOQUES);
	//el archivo ya esta creado con el size maximo
	return file_get_mapped(filename);
}

void data_destroy() {
	munmap(_data, TAMANIO_DATA);
//mapped = NULL;
}

