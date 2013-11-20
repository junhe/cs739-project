# Define required macros here
SHELL = /bin/sh

OBJS=test.o
CFLAGS=-Wall -D_FILE_OFFSET_BITS=64
CC=mpicxx
INCLUDES=
LIBS=-lpthread -lrt
test.x:${OBJS}
	${CC} ${CFLAGS} ${INCLUDES} -o $@ ${OBJS} ${LIBS}

clean:
	-rm -f *.o core *.core *.gch

.cpp.o:
	${CC} ${CFLAGS} ${INCLUDES} -c $<
