#include <stdio.h>

#include "reduce.h"


int main(int argc, char **argv) {
	FILE* stream_in = stdin;
	FILE* stream_out = stdout;


	reduce(stream_in,
		stream_out,
		(int (*)(FILE*, const char const*, void**, int (*)(FILE*, void*)))reduce_static_dictionary_sum,
		(int (*)(FILE*, void*))reduce_static_dictionary_sum_spit);
	fclose(stream_in);
	fclose(stream_out);

	return EXIT_SUCCESS;
}
