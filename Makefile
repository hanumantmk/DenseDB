default: tags AUTOMAKEFILE_DEFAULT

-include AutoMakefile

LFLAGS+= -lprofiler
CFLAGS+= -Wall -Werror -ggdb3 -O3
TARGETS=test_dense_db

clean: AUTOMAKEFILE_CLEAN
	rm -f tags

tags: *.c *.h
	ctags -R --c++-kinds=+p --fields=+iaS --extra=+q

AutoMakefile: *.c *.h Makefile
	./gen_makefile.pl --makefile_name=Makefile -f AutoMakefile $(TARGETS)

