/*
 * mapreduce.h
 *
 *  Created on: 20/5/2015
 *      Author: utnso
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#define LEN_KEYVALUE 1024 //longitud de la key de map o reduce

//#define KEYVALUE_END '_' // para saber cuando termina un archivo, lo marco con esto

typedef struct{
	char ip[15];
	int puerto;
	char archivo[255];//nombre del archivo guardado en tmp
}t_files_reduce;


/* since pipes are unidirectional, we need two pipes.
   one for data to flow from parent's stdout to child's
   stdin and the other for child's stdout to flow to
   parent's stdin */

#define NUM_PIPES          2

#define PARENT_WRITE_PIPE  0
#define PARENT_READ_PIPE   1

int pipes[NUM_PIPES][2];
//int pipes_sort[NUM_PIPES][2];

/* always in a pipe[], pipe[0] is for read and
   pipe[1] is for write */
#define READ_FD  0
#define WRITE_FD 1

#define PARENT_READ_FD  ( pipes[PARENT_READ_PIPE][READ_FD]   )
#define PARENT_WRITE_FD ( pipes[PARENT_WRITE_PIPE][WRITE_FD] )

#define CHILD_READ_FD   ( pipes[PARENT_WRITE_PIPE][READ_FD]  )
#define CHILD_WRITE_FD  ( pipes[PARENT_READ_PIPE][WRITE_FD]  )

bool alguna_key_distinta_null(char** keys, int cant);

int get_index_menor(char** keys, int cant);

void* ejecutar_script(char* script, void* (*procesar_std)()) ;
/*
 * *******************************************************************
 */


void* ejecutar_script(char* script, void* (*procesar_std)()) {
	// pipes for parent to write and read
	if (pipe(pipes[PARENT_READ_PIPE]) == -1)
		handle_error("pipe");

	if ((pipe(pipes[PARENT_WRITE_PIPE])) == -1)
		handle_error("pipe");

	int p =fork();
	if(p==-1)
		handle_error("fork");

	if (p==0) {
		char *argv[] = { script, NULL };
		dup2(CHILD_READ_FD, STDIN_FILENO);
		dup2(CHILD_WRITE_FD, STDOUT_FILENO);
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);
		close(PARENT_READ_FD);
		close(PARENT_WRITE_FD);

		execv(argv[0], argv);
		return NULL;
	} else {
		close(CHILD_READ_FD);
		close(CHILD_WRITE_FD);

		return procesar_std();
	}
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

#endif /* MAPREDUCE_H_ */
