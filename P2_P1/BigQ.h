#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

typedef struct {
    Pipe * in;
    Pipe * out;
    OrderMaker * sortOrder;
    int runlen;
} ThreadData;

class TournamentTree{
    
};

class Runs{
    
public:
    AddPage();
    SortRun();
    RemovePage();
    
};

class RunManager{
    
    
};

class BigQ {
    ThreadData myThreadData;
    TournamentTree myTree;
    pthread_t myThread;
    int totalRuns;
    bool isLastRunComplete;
    DbFile myFile;

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
    Driver(void *arg);
};

#endif
