#include "BigQ.h"

BigQ :: Driver(){
    Phase1();
    getMemoryConfig();
    Phase2();
}

BigQ :: Phase1()
{
    Run myRun(myThreadData.runlen);
    Record readBuffer;
    DBFile tempFile;
    while(myThreadData.in->remove(&readBuffer)){
        myRun.addRecord(&readBuffer);
    }
    myRun.sort();
    tempFile.Open();
    myRun.writeSortedRecords(&DBFile);
    tempFile
    
}

BigQ :: Phase2()
{
    Run myRun(myThreadData.runlen);
    Record readBuffer;
    DBFile tempFile;
    while(myThreadData.in->remove(&readBuffer)){
        myRun.addRecord(&readBuffer);
    }
    myRun.sort();
    tempFile.Open();
    myRun.writeSortedRecords(&DBFile);
    tempFile
    
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
    myThreadData.in = in;
    myThreadData.out = out;
    myThreadData.sortorder = sortorder;
    myThreadData.runlen = runlen;
    pthread_create (&myThread, NULL, Driver);

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
