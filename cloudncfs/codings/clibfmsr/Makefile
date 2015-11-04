all: clibfmsr

clibfmsr:
	swig -python clibfmsr.i
	gcc -O3 -fPIC -std=c99 -c clibfmsr.c clibfmsr_wrap.c -I/usr/include/python2.7
	ld -shared clibfmsr.o clibfmsr_wrap.o -o _clibfmsr.so

clean:
	rm -f *.o *.pyc *.so clibfmsr.py clibfmsr_wrap.c
