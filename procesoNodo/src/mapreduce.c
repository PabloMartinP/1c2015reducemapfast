/*
 * mapreduce.c
 *
 *  Created on: 23/5/2015
 *      Author: utnso
 */


#include "mapreduce.h"

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
