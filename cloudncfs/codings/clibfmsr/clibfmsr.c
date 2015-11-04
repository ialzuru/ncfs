/* File: clibfmsr.c -- currently supports little endian only */

#include "clibfmsr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SOL 8 //sizeof(long), change this accordingly

/* Matrix multiplication, C = AB; A is n x k matrix and B is k x m matrix */
void matMult(const unsigned char *restrict const A,
             const unsigned char *restrict const B,
             char *const C, unsigned char n, unsigned char k, size_t m)
{
    unsigned char row, col;
    const unsigned char *restrict pA = A;
    const register unsigned char *restrict pB;
    char *pC;
    register long *long_pC;
    char *tmp_pC = C;
    const register unsigned char *restrict pGMUL;
    const register unsigned char *lim_pB;
    for (row = 0; row < n; row++, tmp_pC += m) {
        pB = B;
        memset(tmp_pC, 0, m);
        for (col = 0, lim_pB = pB+m-SOL+1; col < k;
             col++, pA++, lim_pB += m-SOL+1) {
            pGMUL = GMUL256[*pA];
            for (long_pC = (long *)tmp_pC; pB < lim_pB;
                 pB += SOL, long_pC++) {
                /* little endian */
                *long_pC ^= ((unsigned long)pGMUL[*pB])           |
                            (((unsigned long)pGMUL[pB[1]])<<8)    |
                            (((unsigned long)pGMUL[pB[2]])<<16)   |
                            (((unsigned long)pGMUL[pB[3]])<<24)
#if (SOL > 4)
                            | (((unsigned long)pGMUL[pB[4]])<<32) |
                            (((unsigned long)pGMUL[pB[5]])<<40)   |
                            (((unsigned long)pGMUL[pB[6]])<<48)   |
                            (((unsigned long)pGMUL[pB[7]])<<56)
#endif
                           ;
            }
            lim_pB += SOL-1;
            for (pC = (char *)long_pC; pB < lim_pB; pB++, pC++) {
                *pC ^= pGMUL[*pB];
            }
        }
    }
}


/* Encoding computation, *c_outdatalist = c_paritycoeff * c_indatalist */
void encodeComputation(char **c_outdatalist, int *len_outdatalist,
                       char *c_indatalist, int len_indatalist,
                       char *c_paritycoeff, int len_paritycoeff,
                       int nativeBlockNum, int parityBlockNum, int chunksize)
{
    *len_outdatalist = parityBlockNum*chunksize;
    *c_outdatalist = (char *) malloc(parityBlockNum*chunksize + SOL);
    matMult(c_paritycoeff, c_indatalist, *c_outdatalist, // A, B, C
        parityBlockNum, nativeBlockNum, chunksize);      // n, k, m
}


/* Decoding computation, *c_outdatalist = c_paritycoeff * c_indatalist */
void decodeComputation(char **c_outdatalist, int *len_outdatalist,
                       char *c_indatalist, int len_indatalist,
                       char *c_paritycoeff, int len_paritycoeff,
                       int nativeBlockNum, int chunksize)
{
    *len_outdatalist = nativeBlockNum*chunksize;
    *c_outdatalist = (char *) malloc(nativeBlockNum*chunksize + SOL);
    matMult(c_paritycoeff, c_indatalist, *c_outdatalist, // A, B, C
        nativeBlockNum, nativeBlockNum, chunksize);      // n, k, m
}


/* Repair computation, *c_outdatalist = c_paritycoeff * c_indatalist */
void repairComputation(char **c_outdatalist, int *len_outdatalist,
                       char *c_indatalist, int len_indatalist,
                       char *c_paritycoeff, int len_paritycoeff,
                       int n, int k, int chunksize)
{
    *len_outdatalist = (n-1)*chunksize;
    *c_outdatalist = (char *) malloc((n-1)*chunksize + SOL);
    matMult(c_paritycoeff, c_indatalist, *c_outdatalist, // A, B, C
        (n-k), (n-1), chunksize);                        // n, k, m
}

