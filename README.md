# gendb



compiling:
gcc -fPIC -rdynamic -c -I/usr/include/postgresql fopenwrap.c
gcc -shared -o libfopenwrap.so fopenwrap.o -lc -ldl -lpq
gcc fopen.c -o fopentest
LD_LIBRARY_PATH=. LD_PRELOAD=libfopenwrap.so ./fopentest fopen.c
