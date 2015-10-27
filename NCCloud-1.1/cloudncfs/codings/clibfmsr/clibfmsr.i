/* File clibfmsr.i */
%module clibfmsr

%include "cstring.i"

%apply (char *STRING, int LENGTH) { (char *c_indatalist, int len_indatalist) };
%apply (char *STRING, int LENGTH) { (char *c_paritycoeff, int len_paritycoeff) };

%cstring_output_allocate_size(char **c_outdatalist, int *len_outdatalist, free(*$1));
void encodeComputation(char **c_outdatalist, int *len_outdatalist,char *c_indatalist, int len_indatalist,char *c_paritycoeff, int len_paritycoeff,int nativeBlockNum,int parityBlockNum,int chunksize);
void decodeComputation(char **c_outdatalist, int *len_outdatalist,char *c_indatalist, int len_indatalist,char *c_paritycoeff, int len_paritycoeff,int nativeBlockNum, int chunksize);
void repairComputation(char **c_outdatalist, int *len_outdatalist,char *c_indatalist, int len_indatalist,char *c_paritycoeff, int len_paritycoeff,int n, int k, int chunksize);

