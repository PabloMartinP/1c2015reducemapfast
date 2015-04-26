/*
 * util.h
 *
 *  Created on: 26/4/2015
 *      Author: utnso
 */

#ifndef UTIL_H_
#define UTIL_H_


#include <stdbool.h>

#define handle_error(msj) \
	do{perror(msj);exit(EXIT_FAILURE);} while(0)

bool file_exists(const char* filename);
void free_null(void* data);
char* file_combine(char* f1, char* f2);
int file_get_size(char* filename);
void* file_get_mapped(char* filename);
void file_mmap_free(void* mapped, char* filename);




#endif /* UTIL_H_ */
