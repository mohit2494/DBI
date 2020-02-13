#include "BigQ.h"


TreeComparator :: TreeComparator(OrderMaker &sortorder){
    myOrderMaker = &sortorder;
}


Run::Run(int runlen) {
    this->runLength = runlen;
}
void Run::AddPage(Page p) {
    this->pages.push_back(p);
}
void Run::sortRun() {
    // sort pages
    // insert pages into tournament
}
vector<Page> Run::getPages() {
    return this->pages;
}
bool Run::checkRunFull() {
    return this->pages.size() == this->runLength;
}
bool Run::clearPages() {
    this->pages.clear();
}
int Run::getRunSize() {
    return this->pages.size();
}

TournamentTree :: TournamentTree(OrderMaker &sortorder,Run &run){
    myOrderMaker = &sortorder;
    myQueue = new priority_queue<Record,vector<Record>,TreeComparator>(TreeComparator(sortorder));
    isRunManagerAvailable = false;
    Inititate(run.getPages());
    
}

TournamentTree :: TournamentTree(OrderMaker &sortorder,RunManager &manager){
    myOrderMaker = &sortorder;
    myRunManager = &manager;
    myQueue = new priority_queue<Record,vector<Record>,TreeComparator>(TreeComparator(sortorder));
    isRunManagerAvailable = true
    Inititate(myRunManager.getPages());

}
void TournamentTree :: Inititate(vector<Page> pages){
    
    for(vector<Page>::iterator page = pages.begin() ; i!=pages.end();++page){
        Record tempRecord;
        page
        
        myQueue->push(
    }
    
}

void* BigQ :: Driver(void *p){
    BigQ * ptr = (BigQ*) p;
    ptr->Phase1();
    ptr->Phase2();
}

void BigQ :: Inititate(){
    
}
void BigQ :: Phase1()
{
    
    
}

void BigQ :: Phase2()
{
    cout <<"Phase2";
    
    //    Run myRun(myThreadData.runlen);
    //    Record readBuffer;
    //    DBFile tempFile;
    //    while(myThreadData.in->remove(&readBuffer)){
    //        myRun.addRecord(&readBuffer);
    //    }
    //    myRun.sort();
    //    tempFile.Open();
    //    myRun.writeSortedRecords(&DBFile);
    //    tempFile
    //
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    myThreadData.in = &in;
    myThreadData.out = &out;
    myThreadData.sortorder = &sortorder;
    myThreadData.runlen = runlen;
    myTree = new TournamentTree(sortorder);
    pthread_create(&myThread, NULL, BigQ::Driver,this);
    pthread_join(myThread,NULL);
    out.ShutDown ();
}

BigQ::~BigQ () {
}
