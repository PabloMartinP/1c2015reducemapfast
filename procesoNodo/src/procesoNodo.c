#include "procesoNodo.h"


int main(int argc, char *argv[]) {
	//por param le tiene que llegar el tamaño del archivo data.bin
	//por ahora hardcodeo 100mb, serian 10 bloques de 20 mb

	inicializar();

	probar_conexion_fs();

	//inicio el server para atender las peticiones del fs
	//iniciar_server_thread();
	incicar_server();

	//todo estas pruebas andan OK 0 leaks
	/*
	//test map OK
	char* timenow = temporal_get_string_time();

	char* file_map1 = string_new();
	string_append(&file_map1, "job_map_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, "_0.txt");
	aplicar_map(0,
			"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/map.sh",
			file_map1);
	printf("%s\n", file_map1);

	char* file_map2 = string_new();
	string_append(&file_map2, "job_map_");
	string_append(&file_map2, timenow);
	string_append(&file_map2, "_1.txt");
	aplicar_map(1,
			"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/map.sh",
			file_map2);
	printf("%s\n", file_map2);
//////////////////////////////////////////////////////////////////
	//test reduce local OK

	char* file_reduce_result = string_new();
	string_append(&file_reduce_result, "job_reduced_result_");
	string_append(&file_reduce_result, timenow);
	t_list* files = list_create();
	list_add(files, (void*) file_map1);
	list_add(files, (void*) file_map2);
	aplicar_reduce_local(files,
			"/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/reduce.sh",
			file_reduce_result);
	list_destroy(files);
	FREE_NULL(file_reduce_result);

//////////////////////////////////////////////////////////////////////////
*/ /*
	 //test reduce de los dos archivos mapeados y en otro nodo?
	 //creo un archivo para reducir
	 t_list* files_to_reduce = list_create();
	 t_files_reduce* file_reduce1 = malloc(sizeof *file_reduce1);
	 //strcpy(file_reduce1->archivo, file_map1);
	 strcpy(file_reduce1->archivo, "6001.txt");
	 strcpy(file_reduce1->ip, NODO_IP());
	 file_reduce1->puerto = NODO_PORT();
	 list_add(files_to_reduce, file_reduce1);

	 //creo otro archivo para reducir
	 t_files_reduce* file_reduce2 = malloc(sizeof *file_reduce2);
	 strcpy(file_reduce2->archivo, "6002.txt");
	 strcpy(file_reduce2->ip, NODO_IP());
	 //file_reduce2->puerto = NODO_PORT();
	 file_reduce2->puerto = 6002;
	 list_add(files_to_reduce, file_reduce2);

	 char* file_red_reduce_result = string_new();
	 string_append(&file_red_reduce_result, "job_reduced_result_");
	 string_append(&file_red_reduce_result, timenow);
	 aplicar_reduce_local_red(files_to_reduce, "/home/utnso/Escritorio/git/tp-2015-1c-dalemartadale/procesoJob/reduce.sh", file_red_reduce_result);
	 printf("%s\n", file_red_reduce_result);

	 free(file_red_reduce_result);
	 list_destroy(files_to_reduce);
	 free(file_reduce1);
	 free(file_reduce2);

	 free(file_reduce_result);


	free(file_map1);
	free(file_map2);

	FREE_NULL(timenow);
*/
	//bool fin = true	;
	while (!FIN);
	finalizar();

	return EXIT_SUCCESS;
}

//files es una lista de t_files_reduce
int aplicar_reduce_local_red(t_list* files_reduces, char*script_reduce,	char* filename_result) {
	int i = 0;
	int rs;
	int cant_red_files, cant_total_files, cant_local_files;
	t_list* local_files = list_create();	//tmp para guardar los fr locales
	t_list* local_files_reduce;// = list_create();	//lista que solo tiene los nombres de los archivos temp
	t_list* red_files;// = list_create();

	//creo ambas listas
	local_files_reduce = list_filter(files_reduces, (void*) file_reduce_es_local);
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
	i = 0;
	void _open_file(char* filename) {
		fdlocal[i] = fopen(filename, "r");
		i++;
	}
	list_iterate(local_files, (void*) _open_file);

	//hasta aca tengo los archivos locales abiertos, listos para empezar a leer
	//ahora tengo que decirle a los nodos que stan en red que me devuelvan su archivo para empezara leer
	int* fdred = malloc(cant_red_files * sizeof(int));
	i = 0;
	void _request_file_to_nodo(t_files_reduce* fr) {
		fdred[i] = client_socket(fr->ip, fr->puerto);
		t_msg* msg = string_message(NODO_GET_FILECONTENT_DATA, fr->archivo, 0);
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
	list_destroy_and_destroy_elements(local_files, (void*)free);
	list_destroy(red_files);
	list_destroy(local_files_reduce);

	int index_menor; //para guardar el menor item
	//creo una lista de key para guardar las key de cada file
	size_t len_key = LEN_KEYVALUE;
	char* key = NULL;
	char** keys = malloc(sizeof(char*) * (cant_total_files));
	int _reduce_local_red() {
		int i = 0;

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
		while (alguna_key_distinta_null(keys, cant_total_files)) {
			//obtengo cual es el menor
			index_menor = get_index_menor(keys, cant_total_files);
			//el menor lo mando a stdinn (keys[i])
			printf("%s\n", keys[index_menor]);
			write(PARENT_WRITE_FD, keys[index_menor], strlen(keys[index_menor]) + 1);
			//leo el siguiente elmento del fdlocal[index_menor]
			len_key = LEN_KEYVALUE;
			if (index_menor < cant_local_files) {
				rs = getline(&(keys[index_menor]), &len_key,fdlocal[index_menor]);
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

		}
		//si llego hasta aca termino de enviarle cosas por stdin,
		//cierro el stdin
		close(PARENT_WRITE_FD);

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

		//limpio las keys -- ya estan limpias
/*
		for (i = 0; i < cant_total_files; i++) {
			free(keys[i]);
		}*/
		//limpio key
		FREE_NULL(keys);
		/////////////////////////////////////////////////
		//empiezo a leer el stdout
		int count;
		char* new_file_reduced;
		new_file_reduced = convertir_a_temp_path_filename(filename_result);	//genero filename absoluto
		FILE* file_reduced = txt_open_for_append(new_file_reduced);	//creo el file
		FREE_NULL(new_file_reduced);		//limpio el nombre
		char* buffer = malloc(LEN_KEYVALUE);//creo un buffer para ir almacenando el stdout
		i = 0;
		do {
			count = read(PARENT_READ_FD, buffer, LEN_KEYVALUE);
			//guardo en el archivo resultado
			fwrite(buffer, count, 1, file_reduced);
			if(count>0){
				buffer[count] = '\0';
				printf("%s\n", buffer);
			}

			i++;
		} while (count != 0);
		close(PARENT_READ_FD);
		fclose(file_reduced);
		FREE_NULL(buffer);

//////////////////////////////////////////////


		return 0;
	}

	return (int) ejecutar_script(script_reduce, (void*) _reduce_local_red);
}

bool file_reduce_es_de_red(t_files_reduce* fr) {
	return !nodo_es_local(fr->ip, fr->puerto);
}
bool file_reduce_es_local(t_files_reduce* fr) {
	return nodo_es_local(fr->ip, fr->puerto);
}

int aplicar_reduce_local(t_list* files, char*script_reduce,	char* filename_result) {
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
			key =  malloc(LEN_KEYVALUE);
			len_key= LEN_KEYVALUE;
			rs = getline(&key, &len_key, fd[i]);
			if(rs!=-1){
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
			write(PARENT_WRITE_FD, keys[index_menor],strlen(keys[index_menor]) + 1);
			//leo el siguiente elmento del fd[index_menor]
			len_key = LEN_KEYVALUE;
			rs = getline(&(keys[index_menor]), &len_key, fd[index_menor]);
			//rs = getline(&key, &len_key, fd[index_menor]);

			//si es igual a -1, termino el file, marco como null la key para que la ignore cuando obtiene el menor
			if (rs == -1) {
				FREE_NULL(keys[index_menor]);
				keys[index_menor] =  NULL;
				//keys[index_menor] = KEYVALUE_END;
			}


		}
		//si llego hasta aca termino de enviarle cosas por stdin,
		//cierro el stdin
		close(PARENT_WRITE_FD);

		int count;
		char* new_file_reduced;
		new_file_reduced = convertir_a_temp_path_filename(filename_result);	//genero filename absoluto
		FILE* file_reduced = txt_open_for_append(new_file_reduced);	//creo el file
		FREE_NULL(new_file_reduced);		//limpio el nombre
		char* buffer = malloc(1024);//creo un buffer para ir almacenando el stdout
		i = 0;
		do {
			count = read(PARENT_READ_FD, buffer, 1024);
			fwrite(buffer, count, 1, file_reduced);
			if(count>0){
				buffer[count] = '\0';
				printf("%s\n", buffer);
			}

			i++;
		} while (count != 0);
		close(PARENT_READ_FD);
		fclose(file_reduced);
		FREE_NULL(buffer);

////////////////////////////////////////
		for (i = 0; i < cant_files; i++)
			fclose(fd[i]);
		FREE_NULL(fd);

		for(i=0;i<cant_files;i++){
			free(keys[i]);
		}
		FREE_NULL(keys);



		return 0;
	}

	return (int) ejecutar_script(script_reduce, (void*) _reduce_local);
}

bool nodo_es_local(char* ip, int puerto) {
	return string_equals_ignore_case(ip, NODO_IP()) && puerto == NODO_PORT();
}

char* convertir_a_temp_path_filename(char* filename) {
	char* new_path_file = string_new();
	string_append(&new_path_file, NODO_DIRTEMP());
	string_append(&new_path_file, "/");
	string_append(&new_path_file, filename);
	return new_path_file;
}

int aplicar_map(int n_bloque, char* script_map, char* filename_result) {
	int _aplicar_map() {
		int res = 0;
		int count;

		// Write to child’s stdin
		char* stdinn = getBloque(n_bloque);

		size_t len = strlen(stdinn);
		if(stdinn[len-1]!='\n'){
			len +=1;
			stdinn= realloc(stdinn, len);
			stdinn[len] = '\0';
			//printf("%s\n", stdinn);
			stdinn[len - 1] = '\n';
		}

		log_trace(logger, "Escribiendo en stdin bloque %d", n_bloque);
		// Read from child’s stdout
		char* new_file_map_disorder = convertir_a_temp_path_filename(filename_result);
		string_append(&new_file_map_disorder, "-disorder.tmp");
		FILE* file_disorder = txt_open_for_append(new_file_map_disorder);

		log_trace(logger, "Empezando a leer stdout y guardar en archivo %s",new_file_map_disorder);
		char* buffer = malloc(LEN_KEYVALUE);

		int bytes_escritos=0;
		int i=0;



		//inicializo en el primer enter
		int len_buff_write=0;
		int len_buff_read=LEN_KEYVALUE;

		if(len<len_buff_write)
			len_buff_write = len;

		do{
			len_buff_write=len_hasta_enter(stdinn + (i*bytes_escritos));
			bytes_escritos = write(PARENT_WRITE_FD, stdinn + (i*bytes_escritos), len_buff_write);
			//printf("faltante > %d\n", len);
			len = len - bytes_escritos;


			count = read(PARENT_READ_FD, buffer, len_buff_read);
			fwrite(buffer, count, 1, file_disorder);
			i++;
			//printf("faltante >>>> %d\n", len);
		}while(len>0);

		if(res==-1){
			log_trace(logger, "Error al escribir en stdin");
			return -1;
		}

		close(PARENT_WRITE_FD);
		log_trace(logger, "Fin escritura stdin bloque %d", n_bloque);
		FREE_NULL(stdinn);



		close(PARENT_READ_FD);
		txt_close_file(file_disorder);
		FREE_NULL(buffer);
		log_trace(logger, "Fin lectura stdout, guardado en archivo %s", new_file_map_disorder);

		log_trace(logger, "Ordenando archivo %s", new_file_map_disorder);
		ordenar_y_guardar_en_temp(new_file_map_disorder, filename_result);//guardo el file definitivo en el tmp
		FREE_NULL(new_file_map_disorder);
		log_trace(logger, "Fin orden, generado archivo final > %s", filename_result);

		return res;
	}

	return (int) ejecutar_script(script_map, (void*) _aplicar_map);
}

int ordenar_y_guardar_en_temp(char* file_desordenado, char* destino) {

	char* commando_ordenar = string_new();
	string_append(&commando_ordenar, "cat ");
	string_append(&commando_ordenar, file_desordenado);
	string_append(&commando_ordenar, " | sort > ");
	string_append(&commando_ordenar, NODO_DIRTEMP());
	string_append(&commando_ordenar, "/");
	string_append(&commando_ordenar, destino);

	//printf("%s\n", commando_ordenar);

	log_trace(logger, "Empezando a ordenar archivo: %s", commando_ordenar);
	system(commando_ordenar);
	log_trace(logger, "Fin Comando ordenar");
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

	char* aux = malloc(strlen(file)+1);
	strcpy(aux, file);
	strcpy(file, cwd);
	strcat(file, aux);
	free(aux);
}

void iniciar_server_thread() {
	pthread_t th;

	//pthread_join(th, NULL);
	pthread_create(&th, NULL, (void*) incicar_server, NULL);

}

char* generar_nombre_script(){
	char* timenow = temporal_get_string_time();
	char* file_map1 = string_new();
	string_append(&file_map1, "job_script_map_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, ".sh");
	free(timenow);
	return file_map1;
}

char* generar_nombre_tmp(){
	char* timenow = temporal_get_string_time();

	char* file_map1 = string_new();
	string_append(&file_map1, "job_script_map_");
	string_append(&file_map1, timenow);
	string_append(&file_map1, ".sh");

	char* tmp ;
	tmp = convertir_a_temp_path_filename(file_map1);
	free(file_map1);
	free(timenow);

	return tmp;
}


void procesar_mensaje(int fd, t_msg* msg) {
	char* bloque;
	char* filename_script;
	int n_bloque = 0;
	//char* buff;
	char* file_data;
	t_map* map = NULL;
	switch (msg->header.id) {
	case JOB_MAPPER:
		destroy_message(msg);
		log_trace(logger, "Recibido nuevo mapper");
		//recibo el map que me envio
		map = recibir_mensaje_map(fd);
		filename_script = generar_nombre_tmp();
		recibir_mensaje_script_y_guardar(fd, filename_script);

		/*
		/////////////////////////////////////////////////////////////////////////////
		//recibo el numero bloque y el nombre del archivo donde guardar el resultado
		//print_msg(msg);
		filename_result = string_new();string_append(&filename_result, msg->stream);
		n_bloque = msg->argv[0];
		tam_script = msg->argv[1];
		log_trace(logger, "Inicio nuevo mapper a aplicar sobre el bloque %d, guardarlo con nombre: %s", n_bloque, msg->stream);
		destroy_message(msg);
		//recibo el script
		script = malloc(tam_script);
		msg = recibir_mensaje(fd);
		//print_msg(msg);
		memcpy(script, msg->stream, tam_script);
		destroy_message(msg);
		//recibir_mensaje_flujo(fd, (void*)&script);
		filename_script = generar_nombre_tmp();
		//grabo el script en tmp
		write_file(filename_script,script, tam_script);
		log_trace(logger, "recibido script tamaño: %d, guardado con nombre: %s", tam_script, filename_script);
		free(script);

		//settear permisos de ejecucion
		chmod(filename_script, S_IRWXU);

		*///////////////////////////////////////////////

		log_trace(logger, "Aplicando mapper %s sobre el bloque %d", filename_script, map->archivo_nodo_bloque->numero_bloque);
		aplicar_map(map->archivo_nodo_bloque->numero_bloque, filename_script, map->info->resultado);
		log_trace(logger, "Fin mapper guardado en %s", map->info->resultado);
		free(filename_script);
		//remove(filename_script);

		msg = argv_message(MAPPER_TERMINO, 0);
		enviar_mensaje(fd, msg);
		log_trace(logger, "Enviado MAPPER_TERMINO al job");
		destroy_message(msg);

		break;
	case FS_AGREGO_NODO:
		log_trace(logger, "El nodo se agrego al fs con id %d", msg->argv[0]);
		destroy_message(msg);

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

		break;
	case FS_GRABAR_BLOQUE:
		bloque = malloc(TAMANIO_BLOQUE_B);
		memcpy(bloque, msg->stream, msg->argv[1]);	//1 es el tamaño real
		memset(bloque + msg->argv[1], '\0', TAMANIO_BLOQUE_B - msg->argv[1]);
		setBloque(msg->argv[0], bloque);
		FREE_NULL(bloque);

		destroy_message(msg);
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

}
void incicar_server() {
	log_trace(logger, "Iniciado server. Puerto: %d\n", NODO_PORT());

	server_socket_select(NODO_PORT(), procesar_mensaje);
}

int NODO_CANT_BLOQUES() {
	return CANT_BLOQUES;
}

void probar_conexion_fs() {

	log_trace(logger, "Conectado al FS ... ");

	int fs;

	if ((fs = client_socket(NODO_IP_FS(), NODO_PORT_FS())) > 0) {
		log_trace(logger, "Conectado");
		//t_msg* msg= id_message(NODO_CONECTAR_CON_FS);
		t_msg* msg = NULL;
		msg = argv_message(NODO_CONECTAR_CON_FS,  0);
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

			msg = string_message(RTA_FS_NODO_QUIEN_SOS, NODO_IP(), 4, NODO_PORT(), NODO_NUEVO(), NODO_CANT_BLOQUES(), NODO_ID());
			if ((enviar_mensaje(fs, msg)) < 0) {
				printf("No se pudo responder a %s",	id_string(FS_NODO_QUIEN_SOS));
				exit(EXIT_FAILURE);
			}
			destroy_message(msg);

			log_trace(logger, "Info enviada al FS");

		} else
			log_trace(logger, "No se pudo conectar con el fs");

		//close(fs);
		log_trace(logger, "Conectado con fs en %s:%d\n", NODO_IP_FS(), NODO_PORT_FS());

		log_trace(logger, "Id: %d, %s:%d, Cant_bloques: %d, bin: %s, dirtmp: %s, nuevo:%d", NODO_ID(), NODO_IP(), NODO_PORT(), NODO_CANT_BLOQUES(), NODO_ARCHIVOBIN(), NODO_DIRTEMP(), NODO_NUEVO());
	} else {
		log_trace(logger, "No se pudo iniciar la escucha con el FS");
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

	/*log_info(logger, "Inicio getFileContent(%s)", filename);
	 char* content = NULL;
	 //creo el espacio para almacenar el archivo
	 char* path = file_combine(NODO_DIRTEMP(), filename);
	 content = malloc(file_get_size(path));

	 content = file_get_mapped(path);

	 FREE_NULL(path);

	 log_info(logger, "Fin getFileContent(%s)", filename);
	 return content;*/
}

void setBloque(int32_t numero, char* bloquedatos) {
	log_info(logger, "Inicio setBloque(%d)", numero);

	//memcpy((void*)(_data[numero*TAMANIO_BLOQUE]), bloquedatos, TAMANIO_BLOQUE);
	memset(_data + (numero * TAMANIO_BLOQUE_B), 0, TAMANIO_BLOQUE_B);
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
	log_info(logger, "Fin getBloque(%d)\n", numero);
	return bloque;
}

//devuelvo el archivo data.bin mappeado
void* data_get(char* filename) {

	if (!file_exists(filename)) {
		TAMANIO_DATA = 1024 * 1024 * NODO_TAMANIO_DATA_DEFAULT_MB(); //100MB
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
	}
	//calculo la cantidad de bloques
	TAMANIO_DATA = file_get_size(filename);
	CANT_BLOQUES = TAMANIO_DATA / TAMANIO_BLOQUE_B;

	log_trace(logger, "Cant-bloques de 20mb: %d", CANT_BLOQUES);
	//el archivo ya esta creado con el size maximo
	return file_get_mapped(filename);
}

void data_destroy() {
	munmap(_data, TAMANIO_DATA);
//mapped = NULL;
}

