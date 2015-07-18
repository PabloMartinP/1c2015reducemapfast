#include "map.h"

int map(FILE* stream_in, FILE* stream_out, int (*iterator)(const char const*, FILE*)){
	char* buffer=NULL;
	size_t size=0;
	ssize_t read=0;
	while ((read = getline(&buffer, &size, stream_in)) != -1) {
		iterator(buffer, stream_out);
	}
	free(buffer);
	return true;
}

int map_count_leters(const char const* input, FILE* stream_out){
	const char* offset=input;
	int tempDic[ABC_LENGTH] = { 0 };
	while( *(offset) ){
		if( islower(*offset) )
			tempDic[(*offset) - 'a']++;
		offset++;
	}
	int i;
	for( i=0; i<ABC_LENGTH; i++){
		fprintf(stream_out, "%c"KEY_VALUE_SEPARATOR"%d\n", 'a'+i, tempDic[i]);
	}
	return true;
}

int map_count_occurrences(const char const* input, FILE* stream_out, const char const * needle){
	const char* offset=input;
	int occurrences = 0;
	while( NULL != (offset = strstr(offset, needle)) ){
		occurrences++;
		offset+=strlen(needle);
	}
	fprintf(stream_out, "%d\n", occurrences);
	return true;
}
