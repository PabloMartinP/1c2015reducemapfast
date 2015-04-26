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



int file_get_size(char* filename){
	struct stat st;
	stat(filename, &st);
	return st.st_size;
}

//une alos dos string con una barra
char* file_combine(char* f1, char* f2){
	char* p =NULL;
	p= string_new();

	string_append(&p, f1);
	string_append(&p, "/");
	string_append(&p, f2);

	return p;

}

void file_mmap_free(void* mapped, char* filename){
	munmap(mapped, file_get_size(filename));
}

/*
 * devuelve el arhivo mappeado modo lectura y escritura
 */
void* file_get_mapped(char* filename){
	//el archivo ya esta creado con el size maximo
	void* mapped=NULL;
	struct stat st;
	int fd=0;
	fd = open(filename, O_RDWR);
	if(fd==-1){
		handle_error("open");
	}

	stat(filename, &st);
	//printf("%ld\n", st.st_size);
	int size = st.st_size;

	mapped = mmap(NULL, size, PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	if(mapped==MAP_FAILED){
		handle_error("mmap");
	}

	return mapped;
}


void free_null(void* data){
	free(data);
	data = NULL;
}

bool file_exists(const char* filename){
	bool rs=true;

	FILE* f = NULL;
	f = fopen(filename, "r");
	if(f!=NULL){
		fclose(f);
		rs = true;
	}
	else
		rs = false;

	return rs;
}
