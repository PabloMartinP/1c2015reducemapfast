/*
 * mapreduce.h
 *
 *  Created on: 20/5/2015
 *      Author: utnso
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include <stdbool.h>
#include <stdlib.h>
#include <util.h>
//#define KEYVALUE_END '_' // para saber cuando termina un archivo, lo marco con esto



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



#endif /* MAPREDUCE_H_ */
