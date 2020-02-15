#include "BigQ.h"
using namespace std;
// ------------------------------------------------------------------
void* BigQ :: Driver(void *p){
    BigQ * ptr = (BigQ*) p;
//    ptr->Phase1();
    ptr->Phase2();
    
}
void BigQ :: Phase1()
{
    
    Record tRec;
    Page *tPage = new Page();                                           // for allocating memory to page
    Run tRun(this->myThreadData.runlen,this->myThreadData.sortorder);   // intializing run
    
    // read data from in pipe sort them into runlen pages
    while(this->myThreadData.in->Remove(&tRec)) {
        if(!tPage->Append(&tRec)) {
            if (tRun.checkRunFull()) {
                tRun.sortRunInternalPages();
                myTree = new TournamentTree(&tRun,this->myThreadData.sortorder);
                tRun.clearPages();
            }
            tRun.AddPage(tPage);
            delete tPage;
            tPage = new Page();
            tPage->Append(&tRec);
        }
    }
    if(tPage->getNumRecs()>0) {
        if (tRun.checkRunFull()) {
            tRun.sortRunInternalPages();
            tRun.clearPages();
        }
        tRun.AddPage(tPage);
        tRun.sortRunInternalPages();
        delete tPage; // delete pointer
    }
    else if(tRun.getRunSize()!=0) {
        tRun.sortRunInternalPages();
        tRun.clearPages();
    }
}

// sort runs from file using Run Manager
void BigQ :: Phase2()
{   
    int noOfRuns = 10;
    int runLength = 10;
    int totalPages = 100;
    char * f_path = "../../dbfiles/lineitem.bin";
    RunManager runManager(noOfRuns,runLength,totalPages,f_path);
    myTree = new TournamentTree(&runManager,this->myThreadData.sortorder);
    Page tempPage;
    while(myTree->GetSortedPage(tempPage)){
        Record tempRecord;
        while(tempPage.GetFirst(&tempRecord)){
            Schema s("catalog","lineitem");
            tempRecord.Print(&s);
            this->myThreadData.out->Insert(&tempRecord);
        }
    }

}

// constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    myThreadData.in = &in;
    myThreadData.out = &out;
    myThreadData.sortorder = &sortorder;
    myThreadData.runlen = runlen;
    pthread_create(&myThread, NULL, BigQ::Driver,this);
    pthread_join(myThread, NULL);
    out.ShutDown ();
}

// destructor
BigQ::~BigQ () {
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
struct recordComparator {
    bool operator() (int i,int j) { return (i<j);}
} myobject;

Run::Run(int runlen) {
    this->runLength = runlen;
    this->sortorder = NULL;
}

Run::Run(int runlen,OrderMaker * sortorder) {
    this->runLength = runlen;
    this->sortorder = sortorder;
}
void Run::AddPage(Page *p) {
    this->pages.push_back(p);
}
void Run::sortRunInternalPages() {
    for(int i=0; i < pages.size(); i++) {
        this->sortSinglePage(pages.at(i));
    }
}
vector<Page*> Run::getPages() {
    return this->pages;
}
void Run::getPages(vector<Page*> * myPageVector) {
    // myPageVector->swap(this->pages);
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

void Run::sortSinglePage(Page *p) {
    if (sortorder){
        vector<Record*> records;
        int numRecs = p->getNumRecs();
        for(int i=0; i<numRecs; i++) {
            records.push_back(new Record());
            p->GetFirst(records.at(i));
        }
        sort(records.begin(), records.end(), CustomComparator(this->sortorder));
        for(int i=0; i<records.size();i++) {
            Record *t = records.at(i);
            t->Print(new Schema("catalog","nation"));
            p->Append(t);
        }
    }
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
TournamentTree :: TournamentTree(Run * run,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    if (myQueue){
           delete myQueue;
    }
    myQueue = new priority_queue<Record*,vector<Record*>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = false;
    run->getPages(&myPageVector);
    Inititate();
    
}

TournamentTree :: TournamentTree(RunManager * manager,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myRunManager = manager;
    if (myQueue){
        delete myQueue;
    }
    myQueue = new priority_queue<Record*,vector<Record*>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = true;
    myRunManager->getPages(&myPageVector);
    Inititate();
}
void TournamentTree :: Inititate(){
    if (!myPageVector.empty()){
        for(vector<Page*>::iterator i = myPageVector.begin() ; i!=myPageVector.end() ; ++i){
           Record * tempRecord = new Record();
           Page * page = *(i);
           if (!page->GetFirst(tempRecord) && isRunManagerAvailable){
               if (myRunManager->getNextPageOfRun(page ,0)){
                    myQueue->push(tempRecord);
               }
           }
           else{
                myQueue->push(tempRecord);
           }
        }
        int flag = 1;
        while(flag) {
           
            Record * r = myQueue->top();
            myQueue->pop();
            
            if (OutputBuffer.Append(r) || myQueue->empty()){
                flag = 0;
            }
        }
    }
}

bool TournamentTree :: GetSortedPage(Page &page){
    Record temp;
    bool isOutputBufferEmpty = true;
    while(OutputBuffer.GetFirst(&temp)){
        page.Append(&temp);
        isOutputBufferEmpty = false;
    }
    if(isOutputBufferEmpty){
        return 0;
    }
    Inititate();
    return 1;
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
RunManager :: RunManager(int noOfRuns,int runLength,int totalPages,char * f_path){
    this->noOfRuns = noOfRuns;
    this->runLength = runLength;
    this->totalPages = totalPages;
    this->f_path = f_path;
    this->file.Open(1,this->f_path);
    int pageOffset = 0;
    for(int i = 0; i<noOfRuns;i++){
        RunFileObject fileObject;
        fileObject.runId = i;
        fileObject.startPage = pageOffset;
        fileObject.currentPage = fileObject.startPage;
        pageOffset+=runLength;
        if (pageOffset <= totalPages){
            fileObject.endPage = pageOffset;
        }
        else{
            fileObject.endPage = pageOffset - (pageOffset-totalPages - 1 );
        }
        runLocation.insert(make_pair(i,fileObject));
    }
    
}

void  RunManager:: getPages(vector<Page*> * myPageVector){
    for(int i = 0; i< noOfRuns ; i++){
        unordered_map<int,RunFileObject>::iterator runGetter = runLocation.find(i);
        if(!(runGetter == runLocation.end())){
            Page * pagePtr = new Page();
            this->file.GetPage(pagePtr,runGetter->second.currentPage);
            runGetter->second.currentPage+=1;
            if(runGetter->second.currentPage>runGetter->second.endPage){
                runLocation.erase(i);
            }
            myPageVector->push_back(pagePtr);
        }
    }
}

bool RunManager :: getNextPageOfRun(Page * page,int runNo){
    unordered_map<int,RunFileObject>::iterator runGetter = runLocation.find(runNo);
    if(!(runGetter == runLocation.end())){
        this->file.GetPage(page,runGetter->second.currentPage);
        runGetter->second.currentPage+=1;
        if(runGetter->second.currentPage>runGetter->second.endPage){
            runLocation.erase(runNo);
        }
        return true;
    }
    return false;
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
CustomComparator :: CustomComparator(OrderMaker * sortorder){
    this->myOrderMaker = sortorder;
}
bool CustomComparator :: operator ()( Record* lhs,  Record* rhs){
    int val = myComparisonEngine.Compare(lhs,rhs,myOrderMaker);
    return (val <=0)? true : false;
}
// ------------------------------------------------------------------






















//day 2


//#include "BigQ.h"
//
//using namespace std;
//
//// ------------------------------------------------------------------
//void* BigQ :: Driver(void *p){
//  BigQ * ptr = (BigQ*) p;
//  ptr->Phase1();
//  ptr->Phase2();
//
//}
//void BigQ :: Phase1()
//{
//    Record tRec;
//    Page tPage;
//    Run tRun(this->myThreadData.runlen,this->myThreadData.sortorder);
//
//    // read data from in pipe sort them into runlen pages
//    while(this->myThreadData.in->Remove(&tRec)) {
//        if(!tPage.Append(&tRec)) {
//            if (tRun.checkRunFull()) {
//                tRun.sortRunInternalPages();
//                myTree = new TournamentTree(&tRun,this->myThreadData.sortorder);
//                tRun.clearPages();
//            }
//            tRun.AddPage(tPage);
//            tPage.EmptyItOut();
//            tPage.Append(&tRec);
//        }
//    }
//
//    if(tPage.getNumRecs()>0) {
//        if (tRun.checkRunFull()) {
//            tRun.sortRunInternalPages();
//            tRun.clearPages();
//        }
//
//        tRun.AddPage(tPage);
//        tRun.sortRunInternalPages();
//        // TODO:: write run to file
//        tPage.EmptyItOut();
//    }
//    else if(tRun.getRunSize()!=0) {
//        tRun.sortRunInternalPages();
//        // TODO:: write run to file
//        tRun.clearPages();
//    }
//    delete myTree;
//    myTree = NULL;
//}
//
//// sort runs from file using Run Manager
//void BigQ :: Phase2()
//{
//    RunManger runManager(totalRuns,this->myThreadData.runlen,&myFile);
//    myTree = new TournamentTree(&runManager,this->myThreadData.sortorder);
//    Page tempPage
//    while(myTree.GetSortedPage(tempPage)){
//        Record tempRecord;
//        while(tempPage.GetFirst(&tempRecord)){
//            this->myThreadData.out->Insert(&tempRecord);
//        }
//    }
//}
//
//// constructor
//BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
//    myThreadData.in = &in;
//    myThreadData.out = &out;
//    myThreadData.sortorder = &sortorder;
//    myThreadData.runlen = runlen;
//    pthread_create(&myThread, NULL, BigQ::Driver,this);
//    out.ShutDown ();
//}
//
//// destructor
//BigQ::~BigQ () {
//    pthread_join(myThread,NULL);
//}
//// ------------------------------------------------------------------
//
//
//// ------------------------------------------------------------------
//struct recordComparator {
//  bool operator() (int i,int j) { return (i<j);}
//} myobject;
//
//Run::Run(int runlen) {
//    this->runLength = runlen;
//    this->sortorder = NULL;
//}
//
//Run::Run(int runlen,OrderMaker * sortorder) {
//    this->runLength = runlen;
//    this->sortorder = sortorder;
//}
//void Run::AddPage(Page &p) {
//    cout<<"herer";
//    this->pages.push_back(p);
//}
//void Run::sortRunInternalPages() {
//    for(int i=0; i < pages.size(); i++) {
//        this->sortSinglePage(pages.at(i));
//    }
//}
//vector<Page> Run::getPages() {
//    return this->pages;
//}
//void Run::getPages(vector<Page> * myPageVector) {
//    myPageVector->swap(this->pages);
//}
//bool Run::checkRunFull() {
//    return this->pages.size() == this->runLength;
//}
//bool Run::clearPages() {
//    this->pages.clear();
//}
//int Run::getRunSize() {
//    return this->pages.size();
//}
//
//Page Run::sortSinglePage(Page p) {
//    if (sortorder){
//        vector<Record> records;
//        Record temp;
//        while(p.getNumRecs()>0) {
//            p.GetFirst(&temp);
//            records.push_back(temp);
//        }
//        sort(records.begin(), records.end(), CustomComparator(this->sortorder));
//    }
//}
//
//// ------------------------------------------------------------------
//
//
//
//// ------------------------------------------------------------------
//TournamentTree :: TournamentTree(Run * run,OrderMaker * sortorder){
//    myOrderMaker = sortorder;
//    myQueue = new priority_queue<Record,vector<Record>,CustomComparator>(CustomComparator(sortorder));
//    isRunManagerAvailable = false;
//    run->getPages(&myPageVector);
//    Inititate();
//
//}
//
//TournamentTree :: TournamentTree(RunManager * manager,OrderMaker * sortorder){
//    myOrderMaker = sortorder;
//    myRunManager = manager;
//    myQueue = new priority_queue<Record,vector<Record>,CustomComparator>(CustomComparator(sortorder));
//    isRunManagerAvailable = true;
//    myRunManager->getPages(&myPageVector);
//    Inititate();
//}
//void TournamentTree :: Inititate(){
//    if (!myPageVector.empty()){
//        int flag = 1;
//        while(flag) {
//            for(vector<Page>::iterator page = myPageVector.begin() ; page!=myPageVector.end() ; ++page){
//                Record tempRecord;
//                if (!page->GetFirst(&tempRecord) && isRunManagerAvailable){
//                    if (myRunManager->getNextPageOfRun(&(*page),0,0)){
//                        myQueue->push(tempRecord);
//                    }
//                }
//                else{
//                    myQueue->push(tempRecord);
//                }
//            }
//            Record r = myQueue->top();
//            myQueue->pop();
//
//            if (OutputBuffer.Append(&r) || !myQueue->empty()){
//                flag = 0;
//            }
//
//        }
//    }
//
//}
//
//bool TournamentTree :: GetSortedPage(Page &page){
//    Record temp;
//    bool isOutputBufferEmpty = true;
//    while(OutputBuffer.GetFirst(&temp)){
//        page.Append(&temp);
//        isOutputBufferEmpty = false;
//    }
//    if(isOutputBufferEmpty){
//        return 0;
//    }
//    return 1;
//}
//
//// ------------------------------------------------------------------
//
//
//RunManager :: RunManager(int noOfRuns,int runLength,DBFile * file){
//
//
//
//}
//
//void  RunManager:: getPages(vector<Page> * myPageVector){
//
//
//};
//bool RunManager :: getNextPageOfRun(Page * page,int runNo,int pageOffset){
//
//};
//
//
//CustomComparator :: CustomComparator(OrderMaker * sortorder){
//    this->myOrderMaker = sortorder;
//}
//bool CustomComparator :: operator ()( Record & lhs,  Record &rhs){
//    return myComparisonEngine.Compare(&lhs,&rhs,myOrderMaker);
//}
//



// my code startes here


//#include "BigQ.h"
//
//
//TreeComparator :: TreeComparator(OrderMaker &sortorder){
//    myOrderMaker = &sortorder;
//}
//
//
//Run::Run(int runlen) {
//    this->runLength = runlen;
//}
//void Run::AddPage(Page p) {
//    this->pages.push_back(p);
//}
//void Run::sortRun() {
//    // sort pages
//    // insert pages into tournament
//}
//vector<Page> Run::getPages() {
//    return this->pages;
//}
//bool Run::checkRunFull() {
//    return this->pages.size() == this->runLength;
//}
//bool Run::clearPages() {
//    this->pages.clear();
//}
//int Run::getRunSize() {
//    return this->pages.size();
//}
//
//TournamentTree :: TournamentTree(OrderMaker &sortorder,Run &run){
//    myOrderMaker = &sortorder;
//    myQueue = new priority_queue<Record,vector<Record>,TreeComparator>(TreeComparator(sortorder));
//    isRunManagerAvailable = false;
//    run.getPages(&myPageVector);
//    Inititate();
//
//}
//
//TournamentTree :: TournamentTree(OrderMaker &sortorder,RunManager &manager){
//    myOrderMaker = &sortorder;
//    myRunManager = &manager;
//    myQueue = new priority_queue<Record,vector<Record>,TreeComparator>(TreeComparator(sortorder));
//    isRunManagerAvailable = true;
//    myRunManager->getPages(&myPageVector);
//    Inititate();
//}
//void TournamentTree :: Inititate(){
//    if (!myPageVector.empty()){
//        int flag = 1;
//        while(flag) {
//            for(vector<Page>::iterator page = myPageVector.begin() ; page!=myPageVector.end() ; ++page){
//                Record tempRecord;
//                if (!page->GetFirst(&tempRecord) && isRunManagerAvailable){
//                    if (myRunManager->getNextPage(page)){
//                        myQueue->push(tempRecord);
//                    }
//                }
//                else{
//                    myQueue->push(tempRecord);
//                }
//            }
//            Record r = myQueue->top();
//            myQueue->pop();
//
//            if (OutputBuffer.Append(&r) || !myQueue->empty()){
//                flag = 0;
//            }
//
//        }
//    }
//
//}
//
//bool TournamentTree :: GetSortedPage(Page &page){
//    Record temp;
//    bool isOutputBufferEmpty = true;
//    while(OutputBuffer.GetFirst(&temp)){
//        page.Append(&temp);
//        isOutputBufferEmpty = false;
//    }
//    if(isOutputBufferEmpty){
//        return 0;
//    }
//    return 1;
//}
//
//void* BigQ :: Driver(void *p){
//    BigQ * ptr = (BigQ*) p;
//    ptr->Phase1();
//    ptr->Phase2();
//}
//
//void BigQ :: Inititate(){
//
//}
//void BigQ :: Phase1()
//{
//
//
//}
//
//void BigQ :: Phase2()
//{
//    cout <<"Phase2";
//
//    //    Run myRun(myThreadData.runlen);
//    //    Record readBuffer;
//    //    DBFile tempFile;
//    //    while(myThreadData.in->remove(&readBuffer)){
//    //        myRun.addRecord(&readBuffer);
//    //    }
//    //    myRun.sort();
//    //    tempFile.Open();
//    //    myRun.writeSortedRecords(&DBFile);
//    //    tempFile
//    //
//}
//
//BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
//    myThreadData.in = &in;
//    myThreadData.out = &out;
//    myThreadData.sortorder = &sortorder;
//    myThreadData.runlen = runlen;
//    myTree = new TournamentTree(sortorder,NULL);
//    pthread_create(&myThread, NULL, BigQ::Driver,this);
//    pthread_join(myThread,NULL);
//    out.ShutDown ();
//}
//
//BigQ::~BigQ () {
//}
