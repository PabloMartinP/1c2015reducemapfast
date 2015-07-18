#include <stdio.h>

#include "map.h"


int main(int argc, char **argv) {
	FILE* stream_in = stdin;
	FILE* stream_out = stdout;

	map(stream_in, stream_out, map_count_leters);

	fclose(stream_in);
	fclose(stream_out);

	return EXIT_SUCCESS;
}
