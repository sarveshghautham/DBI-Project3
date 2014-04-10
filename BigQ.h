#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include<vector>
#include "File.h"
#include "Record.h"
#include<cstdlib>
#include<vector>
#include<cmath>
#include<algorithm>
#include<queue>
#include<sstream>

using namespace std;

void *readRecordsFromPipe(void*);
int GetRecSize (Record* r);

class recordCompare{
private:
	OrderMaker *sortOrder;
public:
    recordCompare(OrderMaker *sortorder)
    {
        sortOrder = sortorder;
    }
    
    bool operator() (const Record * left,const Record * right) const
    {
        //returns a negative number, a 0, or a positive number if left is less than, equal to, or greater than right.
        ComparisonEngine ceng;
        int returnT = ceng.Compare((Record *)left,(Record *) right, sortOrder);
        
        if(returnT < 0)
        {
            
            return true;
        }
        else
        {
            
            return false;
        }
    }
};

class recordComparePQ
{
private:
	OrderMaker *sortOrder;
public:
    recordComparePQ(OrderMaker *sortorder)
    {
        sortOrder = sortorder;
    }
    bool operator() (const pair<Record *, int> pair1, const pair<Record *, int> pair2) const {
        ComparisonEngine ceng;
        int returnT = ceng.Compare(pair1.first,pair2.first,sortOrder);
        if(returnT < 0)
            return false;
        else
            return true;
    }
};

class BigQ
{
    
    friend void *readRecordsFromPipe(void *);
    
public:
    BigQ (Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int runLength);
    ~BigQ();
    
private:
    Pipe& inputPipe;
    Pipe& outputPipe;
    OrderMaker& sortOrder;
    int runLength;
	
	//File f2;
	pthread_t bigQThread;
    
};

#endif
