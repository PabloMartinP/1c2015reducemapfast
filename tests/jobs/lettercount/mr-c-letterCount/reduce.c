#include "reduce.h"

int reduce_simple_sum(const char const* input, void** temp, int (*reduce_spit)(FILE*, void*)){
	if( NULL == *temp ){
		*temp = calloc( 1, sizeof(int) );
	}
	*((int*)*temp) += (int)strtol(input, NULL, 10);
	return true;
}

int reduce_simple_sum_spit(FILE* stream_out, void* temp){
	fprintf( stream_out, "%d\n", *((int*)temp) );
	return true;
}

int reduce_static_dictionary_sum(FILE* stream_out, const char const* input, t_static_dictionary** temp, int (*spit)(FILE*, t_static_dictionary*)){
	if( NULL == *temp ){
		*temp = calloc( 1, sizeof(t_static_dictionary) );
	} else if( (*temp)->dictionary[(*temp)->size].character != input[0] ){
		spit( stream_out, *temp );
		(*temp)->size++;
	}

	(*temp)->dictionary[(*temp)->size].character = input[0];
	(*temp)->dictionary[(*temp)->size].repetitions += (int)strtol(input + 1 + strlen(KEY_VALUE_SEPARATOR), NULL, 10);

	return true;
}

int reduce_static_dictionary_sum_spit(FILE* stream_out, t_static_dictionary* kv){
	fprintf(stream_out, "%c"KEY_VALUE_SEPARATOR"%d\n", kv->dictionary[kv->size].character,  kv->dictionary[kv->size].repetitions );
	return true;
}

int reduce(FILE* stream_in, FILE* stream_out, int (*reduce_iterator)(FILE*, const char const*, void**, int (*)(FILE*, void*)), int (*reduce_spit)(FILE*, void*)){
	char* buffer=NULL;
	size_t size=0;
	ssize_t read=0;

	void* temp = NULL;

	while ((read = getline(&buffer, &size, stream_in)) != -1) {
		reduce_iterator(stream_out, buffer, &temp, reduce_spit);
	}
	reduce_spit(stream_out, temp);
	free(buffer);
	return true;
}

