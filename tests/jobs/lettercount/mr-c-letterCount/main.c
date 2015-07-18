#include <stdio.h>

#include "reduce.h"
#include "map.h"


int main(int argc, char **argv) {
	FILE* stream_in = stdin;
	FILE* stream_out = stdout;


	if( argc>1 ){
		if( !strncmp( argv[1], "-m", 2 ) ){ //Map
			map(stream_in, stream_out, map_count_leters);
		}else if( !strncmp( argv[1], "-r", 2 ) ){ //Reduce
			reduce(stream_in,
					stream_out,
					(int (*)(FILE*, const char const*, void**, int (*)(FILE*, void*)))reduce_static_dictionary_sum,
					(int (*)(FILE*, void*))reduce_static_dictionary_sum_spit);
		}
	} else {
		fprintf(stderr, "Se debe ejecutar con \"-m\" o \"-r\"\n");
	}

	fclose(stream_in);
	fclose(stream_out);

	return EXIT_SUCCESS;
}
