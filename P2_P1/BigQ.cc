#include "BigQ.h"
#include "Utilities.h"
using namespace std;

// ------------------------------------------------------------------
void* BigQ :: Driver(void *p){
  BigQ * ptr = (BigQ*) p;
  ptr->Phase1();
  ptr->Phase2();

}
void BigQ :: Phase1()
{
    Record tRec;
    Run tRun(this->myThreadData.runlen,this->myThreadData.sortorder);
    
    // add 1 page for adding records
    long long int pageCount=0;
    long long int runCount=1;
    tRun.AddPage();

    // read data from in pipe sort them into runlen pages
    while(this->myThreadData.in->Remove(&tRec)) {
        if(!tRun.addRecordAtPage(pageCount, &tRec)) {
            if (tRun.checkRunFull()) {
                tRun.sortRunInternalPages();
                sortCompleteRun(&tRun);
                tRun.writeRunToFile(&this->myFile);
                tRun.clearPages();pageCount=-1;runCount++;
            }
            tRun.AddPage();pageCount++;
            tRun.addRecordAtPage(pageCount, &tRec);
        }
    }
    if(tRun.getRunSize()!=0) {
        tRun.sortRunInternalPages();
        sortCompleteRun(&tRun);
        tRun.writeRunToFile(&this->myFile);
        tRun.clearPages();
    }
    this->f_path = "temp.bin";
    this->totalRuns = runCount;
}

// sort runs from file using Run Manager
void BigQ :: Phase2()
{
    RunManager runManager(this->totalRuns,this->myThreadData.runlen,this->f_path);
    myTree = new TournamentTree(&runManager,this->myThreadData.sortorder);
    Page * tempPage;
    while(myTree->GetSortedPage(&tempPage)){
        Record tempRecord;
        while(tempPage->GetFirst(&tempRecord)){
            Schema s("catalog","partsupp");
            tempRecord.Print(&s);
            this->myThreadData.out->Insert(&tempRecord);
        }
        myTree->RefillOutputBuffer();
    }
}

void BigQ::sortCompleteRun(Run *run) {
    myTree = new TournamentTree(run,this->myThreadData.sortorder);
    Page * tempPage;
    // as run was swapped by tournament tree
    // we need to allocate space for pages again
    // these pages will be part of complete sorted run
    // add 1 page for adding records
    while(myTree->GetSortedPage(&tempPage)){
//        cout<<tempPage->getNumRecs();
        Record tempRecord;
        Page * pushPage = new Page();
        while(tempPage->GetFirst(&tempRecord)){
            //    Schema s("catalog","part");
            //    tempRecord.Print(&s);
            pushPage->Append(&tempRecord);
        }
        run->AddPage(pushPage);
        myTree->RefillOutputBuffer();
    }
    delete myTree;myTree=NULL;
}

// constructor
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    myThreadData.in = &in;
    myThreadData.out = &out;
    myThreadData.sortorder = &sortorder;
    myThreadData.runlen = runlen;
    myTree=NULL;f_path=NULL;
    pthread_create(&myThread, NULL, BigQ::Driver,this);
    pthread_join(myThread, NULL);
    out.ShutDown ();
}

// destructor
BigQ::~BigQ () {
}
// ------------------------------------------------------------------

// ------------------------------------------------------------------
Run::Run(int runlen) {
    this->runLength = runlen;
    this->sortorder = NULL;
}
Run::Run(int runlen,OrderMaker * sortorder) {
    this->runLength = runlen;
    this->sortorder = sortorder;
}
void Run::AddPage() {
    this->pages.push_back(new Page());
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
    myPageVector->swap(this->pages);
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
            // t->Print(new Schema("catalog","part"));
            p->Append(t);
        }
    }
}
int Run::addRecordAtPage(long long int pageCount, Record *rec) {
    return this->pages.at(pageCount)->Append(rec);
}
bool Run::writeRunToFile(DBFile *file) {
    //TODO::change second parameter to sorted
    //TODO::handle create for this
    cout<<"Writing Files to Run"<<endl;
    if(!Utilities::checkfileExist("temp.bin")) {
        file->Create("temp.bin",heap,NULL);
    }
    else{
        file->Open("temp.bin");

    }
    cout<<pages.size();
    for(int i=0;i<this->pages.size();i++) {
        Record temp;
        while(pages.at(i)->GetFirst(&temp)) {
            temp.Print(new Schema("catalog","partsupp"));
            file->Add(temp);
        }
    }
    file->Close();
    cout<<"Run over"<<endl;

}
// ------------------------------------------------------------------

// ------------------------------------------------------------------
TournamentTree :: TournamentTree(Run * run,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myQueue = new priority_queue<QueueObject,vector<QueueObject>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = false;
    run->getPages(&myPageVector);
    Inititate();
}

TournamentTree :: TournamentTree(RunManager * manager,OrderMaker * sortorder){
    myOrderMaker = sortorder;
    myRunManager = manager;
    myQueue = new priority_queue<QueueObject,vector<QueueObject>,CustomComparator>(CustomComparator(sortorder));
    isRunManagerAvailable = true;
    myRunManager->getPages(&myPageVector);
    Inititate();
}
void TournamentTree :: Inititate(){
    // Schema s("catalog","part");
    if (!myPageVector.empty()){
        int runId = 0;
        for(vector<Page*>::iterator i = myPageVector.begin() ; i!=myPageVector.end() ; ++i){
            Page * page = *(i);
            QueueObject object;
            object.record = new Record();
            object.runId = runId++;
            if(page->GetFirst(object.record)){
                if(isRunManagerAvailable){
                    cout<<"RunID:"<<object.runId<<endl;
                    object.record->Print(new Schema("catalog","partsupp"));
                }
                myQueue->push(object);
            }
        }

        while(!myQueue->empty()){
            QueueObject topObject = myQueue->top();
            if(isRunManagerAvailable){
                cout<<"RunID:"<<topObject.runId<<endl;
                topObject.record->Print(new Schema("catalog","partsupp"));
            }
            
            bool OutputBufferFull = !(OutputBuffer.Append(topObject.record));
                        if (OutputBufferFull){
                break;
            }
            myQueue->pop();
            Page * topPage = myPageVector.at(topObject.runId);
            if (!(topPage->GetFirst(topObject.record))){
                if (isRunManagerAvailable&&myRunManager->getNextPageOfRun(topPage,topObject.runId)){
                    if(topPage->GetFirst(topObject.record)){
                        myQueue->push(topObject);
                    }
                }
            }
            else{
                myQueue->push(topObject);
            }
        }

    }
}

void TournamentTree :: RefillOutputBuffer(){
    // Schema s("catalog","part");
    if (!myPageVector.empty()){
        int runId = 0;
        while(!myQueue->empty()){
            QueueObject topObject = myQueue->top();
            bool OutputBufferFull = !(OutputBuffer.Append(topObject.record));
            if (OutputBufferFull){
                break;
            }
            myQueue->pop();

            Page * topPage = myPageVector.at(topObject.runId);
            if (!(topPage->GetFirst(topObject.record))){
                if (isRunManagerAvailable&&myRunManager->getNextPageOfRun(topPage,topObject.runId)){
                    if(topPage->GetFirst(topObject.record)){
//                        topObject.record->Print(&s);
//                        cout<<topObject.runId;
                        myQueue->push(topObject);
                    }
                }
            }
            else{
//                topObject.record->Print(&s);
//                cout<<topObject.runId;
                myQueue->push(topObject);
            }

        }
    }
}



bool TournamentTree :: GetSortedPage(Page ** page){
    if (OutputBuffer.getNumRecs()>0){
        *(page) = &OutputBuffer;
        return 1;
    }
    return 0;
}
// ------------------------------------------------------------------


// ------------------------------------------------------------------
RunManager :: RunManager(int noOfRuns,int runLength,char * f_path){
    this->noOfRuns = noOfRuns;
    this->runLength = runLength;
    this->f_path = f_path;
    this->file.Open(1,this->f_path);
    int totalPages = file.GetLength()-1;
    this->totalPages = totalPages;
    int pageOffset = 0;
    for(int i = 0; i<noOfRuns;i++){
        RunFileObject fileObject;
        fileObject.runId = i;
        fileObject.startPage = pageOffset;
        fileObject.currentPage = fileObject.startPage;
        pageOffset+=runLength;
        if (pageOffset <=totalPages){
            fileObject.endPage = pageOffset-1;
        }
        else{
            fileObject.endPage =  fileObject.startPage + (totalPages-fileObject.startPage-1);
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
//        cout<<runGetter->second.currentPage<<runGetter->second.endPage;
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
bool CustomComparator :: operator ()( QueueObject lhs, QueueObject rhs){
    int val = myComparisonEngine.Compare(lhs.record,rhs.record,myOrderMaker);
    return (val <=0)? false : true;
}

bool CustomComparator :: operator ()( Record* lhs, Record* rhs){
    int val = myComparisonEngine.Compare(lhs,rhs,myOrderMaker);
    return (val <0)? true : false;
}
// ------------------------------------------------------------------

