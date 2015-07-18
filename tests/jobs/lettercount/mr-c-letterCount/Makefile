all:
	make mapper
	make reducer

clean:
	touch mapper
	touch reducer
	rm mapper
	rm reducer

mapper:
	gcc mapper.c map.c -o mapper

reducer:
	gcc reducer.c reduce.c -o reducer

