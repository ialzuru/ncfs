/*File clibfmsr.h*/

#ifndef CLIBFMSR_H
#define CLIBFMSR_H
#include <string.h>
#include "GF256mult.h" //GF256 multiplication table

void matMult(const unsigned char *restrict const A,
             const unsigned char *restrict const B,
             char *const C, unsigned char n, unsigned char k, size_t m);

/* void genCoeff(unsigned char *A, unsigned char n, unsigned char k); */

void encodeComputation(char **c_outdatalist, int *len_outdatalist,
                       char *c_indatalist, int len_indatalist,
                       char *c_paritycoeff, int len_paritycoeff,
                       int nativeBlockNum, int parityBlockNum, int chunksize);

void decodeComputation(char **c_outdatalist, int *len_outdatalist,
                       char *c_indatalist, int len_indatalist,
                       char *c_paritycoeff, int len_paritycoeff,
                       int nativeBlockNum, int chunksize);

void repairComputation(char **c_outdatalist, int *len_outdatalist,
                       char *c_indatalist, int len_indatalist,
                       char *c_paritycoeff, int len_paritycoeff,
                       int n, int k, int chunksize);

#endif /* CLIBFMSR_H */
