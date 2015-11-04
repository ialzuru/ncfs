#!/usr/bin/python
#
# @name = 'coeffvector.py'
# 
# @description = "Class of coefficient vector."
#
# @author = ['YU Chiu Man', 'HU Yuchong', 'TANG Yang']
#

import sys, os, random
from finitefield import GF256int

class CoeffVector:
    '''Class of coefficient vector.'''
    first_ = -1                     # index of the first non-zero coefficient
    k_ =  0                         # number of coefficients
    coeff_=[];                      # list of coefficients    
    
    def __init__(self, nativeBlockNum):
        self.k_ = nativeBlockNum
        self.coeff_=[GF256int(0)]*self.k_
        for i in range (0,self.k_):
            if self.first_ == -1 and self.coeff_[i] != 0:
                self.first_=i
        
    def normalize(self):
        '''Normalize coefficient vector.'''
        if self.first_ == -1: return     #unable to nornalize
        a=self.coeff_[self.first_]
        for i in range(self.first_,self.k_):
            self.coeff_[i]=self.coeff_[i] / a
                        
    def first(self):
        '''First insert of vector.'''
        self.first_ = -1
        for i in range (0,self.k_):
            if self.first_ == -1 and self.coeff_[i] != 0:
                self.first_=i
    
    def copy(self):
        '''Copy vector.'''
        newone = CoeffVector(self.k_)
        newone.first_ = self.first_
        newone.k_ = self.k_
        for i in range (self.k_):       
            newone.coeff_[i] = self.coeff_[i]
        return newone


class CoeffMatrix:
    '''Class of coefficient matrix.'''
    rank_ = 0                                       # current rank of the matrix
    k_ =  0                                         # matrix size, i.e., k_ * k_
    coeff_=[];                                      # current coefficients in vectors (in normalized form)  
    rev_matrix = []                                 # reversed matrix
    
    def __init__(self, nativeBlockNum):
        self.k_ = nativeBlockNum
        for i in range (0,self.k_):
            self.coeff_.append(CoeffVector(self.k_))
            self.rev_matrix.append(CoeffVector(self.k_))     
        for n in range(self.k_):
            for m in range(self.k_):  
                if m==n:  self.rev_matrix[n].coeff_[m]= GF256int(1)     
    
    def reset(self):
        '''Reset matrix such that all coefficient becomes 1.'''
        self.rank_ = 0                                       # current rank of the matrix
        for i in range (0,self.k_):
            self.coeff_.append(CoeffVector(self.k_))
            self.rev_matrix.append(CoeffVector(self.k_))     
        for n in range(self.k_):
            for m in range(self.k_):  
                if m==n:  self.rev_matrix[n].coeff_[m]= GF256int(1) 
                
    def printmatrix(self):
        '''Print matrix.'''
        for n in range(self.k_):
            for m in range(self.k_):        
                print self.coeff_[n].coeff_[m],
            print ''
        
    def addcoeffvector(self, newcoeffvector):
        '''Add coefficient vector to matrix.'''
        newcoeffvector.first()
        #self.printmatrix()
        if newcoeffvector.first_ < 0: 
            '''The new coeffvector is a zero vector, so drop it'''
            return False
        if self.rank_==0:
            'The first non-zero vector'
            newcoeffvector.normalize()
            self.coeff_[0]=newcoeffvector
            self.rank_=self.rank_+1
            return True
        elif self.rank_==self.k_: 
            '''The matrix has been full rank! No vector needs to be added'''
            return False
        else :                                          # 1 <= rank < k_
            for i in range(0,self.rank_):
                if self.coeff_[i].first_ >  newcoeffvector.first_ and newcoeffvector.first_ >= 0:
                    '''The new coeffvector gives the current matrix a new base'''
                    for j in range(self.rank_,i,-1):
                        self.coeff_[j]=self.coeff_[j-1]
                    newcoeffvector.normalize()
                    self.coeff_[i]=newcoeffvector
                    self.rank_=self.rank_+1
                    return True
                elif   self.coeff_[i].first_ == newcoeffvector.first_ and newcoeffvector.first_ >= 0:
                    #Gauss elimination:
                    h = newcoeffvector.coeff_[newcoeffvector.first_]
                    for m in range(self.k_):      
                        newcoeffvector.coeff_[m] = newcoeffvector.coeff_[m] - self.coeff_[i].coeff_[m]*h 
                    #Update newcoeffvector.first_:
                    newcoeffvector.first_= -1
                    for i in range (0,newcoeffvector.k_):           
                        if newcoeffvector.first_ == -1 and newcoeffvector.coeff_[i] != 0:               
                            newcoeffvector.first_=i
                else:  
                    pass                    
                if newcoeffvector.first_ < 0: 
                    '''Independency Checking finish! The new coeffvector is redunced to zero one, so drop it'''
                    return False
                
            '''Though the new coeffvector cannot give the current matrix a new base, it gives the current matrix a new combination.'''
            newcoeffvector.normalize()
            self.coeff_[self.rank_]=newcoeffvector
            '''NO dependency'''
            self.rank_=self.rank_+1
            return True
