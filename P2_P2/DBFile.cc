#include "DBFile.h"
#include "Utilities.h"

/*-----------------------------------------------------------------------------------*/
GenericDBFile::GenericDBFile(){
    
}
GenericDBFile::~GenericDBFile(){

}

int GenericDBFile::GetPageLocationToWrite() {
    int pageLocation = myFile.GetLength();
    return !pageLocation ? 0 : pageLocation-1;
}

int GenericDBFile::GetPageLocationToRead(BufferMode mode) {
    if (mode == WRITE){
        return myPreferencePtr->currentPage-2;
    }
    else if (mode == READ){
        return myPreferencePtr->currentPage;
    }
}

int GenericDBFile::GetPageLocationToReWrite(){
    int pageLocation = myFile.GetLength();
    return pageLocation == 2 ? 0 : pageLocation-2;

}

void GenericDBFile::Create(char * f_path,fType f_type, void *startup){
    // opening file with given file extension
    myFile.Open(0,(char *)f_path);
    if (startup!=NULL and f_type==sorted){
        myPreferencePtr->orderMaker = ((SortedStartUp *)startup)->o;
        myPreferencePtr->runLength  = ((SortedStartUp *)startup)->l;
    }
}

int GenericDBFile::Open(char * f_path){
    // opening file with given file extension
    myFile.Open(1,(char *)f_path);
    if(myFile.IsFileOpen()){
        // Load the last saved state from preference.
        if( myPreferencePtr->pageBufferMode == READ){
            myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
            Record myRecord;
            for (int i = 0 ; i < myPreferencePtr->currentRecordPosition; i++){
                myPage.GetFirst(&myRecord);
            }
            myPreferencePtr->currentPage++;
        }
        else if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->isPageFull){
            myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
        }
        return 1;
    }
    return 0;
}

void GenericDBFile::MoveFirst () {
    if (myFile.IsFileOpen()){
        if (myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!myPreferencePtr->allRecordsWritten){
                myFile.AddPage(&myPage,GetPageLocationToReWrite());
            }
        }
        myPage.EmptyItOut();
        myPreferencePtr->pageBufferMode = READ;
        myFile.MoveToFirst();
        myPreferencePtr->currentPage = 0;
        myPreferencePtr->currentRecordPosition = 0;
    }
}

void GenericDBFile::Add(Record &addme){}
void GenericDBFile::Load(Schema &myschema, const char *loadpath){}
int GenericDBFile::GetNext(Record &fetchme){}
int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){}
int GenericDBFile::Close(){}

/*-----------------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------------*/
HeapDBFile :: HeapDBFile(Preference * preference){
    myPreferencePtr = preference;
}

void HeapDBFile :: Add (Record &rec) {

    if (!myFile.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

     // Flush the page data from which you are reading and load the last page to start appending records.
     if (myPreferencePtr->pageBufferMode == READ ) {
            if( myPage.getNumRecs() > 0){
                myPage.EmptyItOut();
            }
            // open page the last written page to start rewriting
            myFile.GetPage(&myPage,GetPageLocationToReWrite());
            myPreferencePtr->currentPage = GetPageLocationToReWrite();
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
            myPreferencePtr->reWriteFlag = true;
    }

    // set DBFile in write mode
    myPreferencePtr->pageBufferMode = WRITE;

    if(myPage.getNumRecs()>0 && myPreferencePtr->allRecordsWritten){
                    myPreferencePtr->reWriteFlag = true;
    }

    // add record to current page
    // check if the page is full
    if(!this->myPage.Append(&rec)) {

        //cout << "DBFile page full, writing to disk ...." << myFile.GetLength() << endl;

        // if page is full, then write page to disk. Check if the date needs to rewritten or not
        if (myPreferencePtr->reWriteFlag){
            this->myFile.AddPage(&this->myPage,GetPageLocationToReWrite());
            myPreferencePtr->reWriteFlag = false;
        }
        else{
            this->myFile.AddPage(&this->myPage,GetPageLocationToWrite());
        }

        // empty page
        this->myPage.EmptyItOut();

        // add again to page
        this->myPage.Append(&rec);
    }
    myPreferencePtr->allRecordsWritten=false;
}

void HeapDBFile :: Load (Schema &f_schema, const char *loadpath) {

    if (!myFile.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

    Record temp;
    // Flush the page data from which you are reading and load the last page to start appending records.
    if (myPreferencePtr->pageBufferMode == READ ) {
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        // open page for write
        myFile.GetPage(&myPage,GetPageLocationToReWrite());
        myPreferencePtr->currentPage = GetPageLocationToReWrite();
        myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
        myPreferencePtr->reWriteFlag = true;
    }
    // set DBFile in WRITE Mode
    myPreferencePtr->pageBufferMode = WRITE;
    FILE *tableFile = fopen (loadpath, "r");
    // while there are records, keep adding them to the DBFile. Reuse Add function.
    while(temp.SuckNextRecord(&f_schema, tableFile)==1) {
        Add(temp);
    }

}

int HeapDBFile :: GetNext (Record &fetchme) {
    if (myFile.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if (myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!myPreferencePtr->allRecordsWritten){
                myFile.AddPage(&myPage,GetPageLocationToReWrite());
            }
            //  Only Write Records if new records were added.
            myPage.EmptyItOut();
            myPreferencePtr->currentPage = myFile.GetLength();
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
            return 0;
        }
        myPreferencePtr->pageBufferMode = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!myPage.GetFirst(&fetchme)) {
            // check if all records are read.
            if (myPreferencePtr->currentPage+1 >= myFile.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
                myPage.GetFirst(&fetchme);
                myPreferencePtr->currentPage++;
                myPreferencePtr->currentRecordPosition = 0;
            }
        }
        // increament counter for each read.
        myPreferencePtr->currentRecordPosition++;
        return 1;
    }
}

int HeapDBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
        // Flush the Page Buffer if the WRITE mode was active.
        if (myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            //  Only Write Records if new records were added.
            if(!myPreferencePtr->allRecordsWritten){
                myFile.AddPage(&myPage,GetPageLocationToReWrite());
            }
            myPage.EmptyItOut();
            myPreferencePtr->currentPage = myFile.GetLength();
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
            return 0;
        }
        bool readFlag ;
        bool compareFlag;
        // loop until all records are read or if some record maches the filter CNF
        do{
            readFlag = GetNext(fetchme);
            compareFlag = myCompEng.Compare (&fetchme, &literal, &cnf);
        }
        while(readFlag && !compareFlag);
        if(!readFlag){
            return 0;
        }
        return 1;

}

int HeapDBFile :: Close () {
    if (!myFile.IsFileOpen()) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    if(myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!myPreferencePtr->allRecordsWritten){
                if (myPreferencePtr->reWriteFlag){
                    myFile.AddPage(&this->myPage,GetPageLocationToReWrite());
                    myPreferencePtr->reWriteFlag = false;
                }
                else{
                    myFile.AddPage(&this->myPage,GetPageLocationToWrite());
                }
            }
            myPreferencePtr->isPageFull = false;
            myPreferencePtr->currentPage = myFile.Close();
            myPreferencePtr->allRecordsWritten = true;
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
    }
    else{
        if(myPreferencePtr->pageBufferMode == READ){
            myPreferencePtr->currentPage--;
        }
        myFile.Close();
    }
    return 1;
}

HeapDBFile::~HeapDBFile(){

}

/*-----------------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------------*/
SortedDBFile :: SortedDBFile(Preference * preference){
    myPreferencePtr = preference;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
}
SortedDBFile::~SortedDBFile(){

}
void SortedDBFile :: Add(Record &addme){
//    cout<<"Add"<<endl;
    if (!myFile.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }
    
    
    // assign new pipe instance for input pipe if null
         if (inputPipePtr == NULL){
              inputPipePtr = new Pipe(10);
         }
         if(outputPipePtr == NULL){
             outputPipePtr = new Pipe(10);
         }
         if(bigQPtr == NULL){
            bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(myPreferencePtr->orderMaker), myPreferencePtr->runLength);
         }
     
    
     // Flush the page data from which you are reading and load the last page to start appending records.
     if (myPreferencePtr->pageBufferMode == READ ) {
            if( myPage.getNumRecs() > 0){
                myPage.EmptyItOut();
            }
         // assign new pipe instance for input pipe if null
         if (inputPipePtr == NULL){
              inputPipePtr = new Pipe(10);
         }
         if(outputPipePtr == NULL){
             outputPipePtr = new Pipe(10);
         }
         if(bigQPtr == NULL){
            bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(myPreferencePtr->orderMaker), myPreferencePtr->runLength);
         }
    }
    

    // set DBFile in write mode
    myPreferencePtr->pageBufferMode = WRITE;
    
    // add record to input pipe
    inputPipePtr->Insert(&addme);
    
    // set allrecords written as false
    myPreferencePtr->allRecordsWritten=false;
}
void SortedDBFile :: Load(Schema &myschema, const char *loadpath){
    cout<<"Load"<<endl;
    if (!myFile.IsFileOpen()){
          cerr << "Trying to load a file which is not open!";
          exit(1);
      }

      Record temp;
      // Flush the page data from which you are reading and load the last page to start appending records.
       if (myPreferencePtr->pageBufferMode == READ ) {
              if( myPage.getNumRecs() > 0){
                  myPage.EmptyItOut();
              }
              // assign new pipe instance for input pipe if null
               if (inputPipePtr == NULL){
                    inputPipePtr = new Pipe(10);
               }
      }
    
      // set DBFile in WRITE Mode
      myPreferencePtr->pageBufferMode = WRITE;
      FILE *tableFile = fopen (loadpath, "r");
      // while there are records, keep adding them to the DBFile. Reuse Add function.
      while(temp.SuckNextRecord(&myschema, tableFile)==1) {
          Add(temp);
      }
}
int SortedDBFile :: GetNext(Record &fetchme){
    if (myFile.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->allRecordsWritten){
                   MergeSortedInputWithFile();
        }
        myPreferencePtr->pageBufferMode = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!myPage.GetFirst(&fetchme)) {
            // check if all records are read.
            if (myPreferencePtr->currentPage+1 >= myFile.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
                myPage.GetFirst(&fetchme);
                myPreferencePtr->currentPage++;
                myPreferencePtr->currentRecordPosition = 0;
            }
        }
        // increament counter for each read.
        myPreferencePtr->currentRecordPosition++;
        return 1;
    }
}
int SortedDBFile :: GetNext(Record &fetchme, CNF &cnf, Record &literal){
    cout<<"GetNext CNF"<<endl;

}
int SortedDBFile :: Close(){
//    cout<<"Close"<<endl;
    if (!myFile.IsFileOpen()) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    
    if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->allRecordsWritten){
            MergeSortedInputWithFile();
            myPreferencePtr->isPageFull = false;
            myPreferencePtr->currentPage = myFile.Close();
            myPreferencePtr->allRecordsWritten = true;
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
    }
    else{
        if(myPreferencePtr->pageBufferMode == READ){
            myPreferencePtr->currentPage--;
        }
        myFile.Close();
    }
    return 1;
    
}
void SortedDBFile::MergeSortedInputWithFile(){
    // setup for new file
    string fileName(myPreferencePtr->preferenceFilePath);
    string newFileName = fileName.substr(0,fileName.find_last_of('.'))+".nbin";
    char* new_f_path = new char[newFileName.length()+1];
    strcpy(new_f_path, newFileName.c_str());
    newFile.Open(0,new_f_path);
    Page page;
    int newFilePageCounter = 0;
    
    // shut down input pipe;
    inputPipePtr->ShutDown();
    
    // flush the read buffer
    if(myPage.getNumRecs() > 0){
        myPage.EmptyItOut();
    }
    
    // page counts;
    int totalPages = myFile.GetLength()-1;
    int currentPage = 0;
    // set flags to initiate merge
    bool fileReadFlag = true;
    bool outputPipeReadFlag = true;
    
    bool getNextFileRecord = true;
    bool getNextOutputPipeRecord = true;
    Record * fileRecordptr = NULL;
    Record * outputPipeRecordPtr = NULL;
    while(fileReadFlag || outputPipeReadFlag){
        if (getNextOutputPipeRecord){
            outputPipeRecordPtr = new Record();
            getNextOutputPipeRecord = false;
            if(!outputPipePtr->Remove(outputPipeRecordPtr)){
                cout<<"outputPipePtr over"<<endl;
                outputPipeReadFlag= false;
            }
        }
        
        if (getNextFileRecord){
            fileRecordptr=new Record();
            getNextFileRecord = false;
            if(!myPage.GetFirst(fileRecordptr)){
                if(currentPage<totalPages){
                    myFile.GetPage(&myPage,currentPage);
                    myPage.GetFirst(fileRecordptr);
                    currentPage++;
                }
                else{
                    fileReadFlag= false;
                }
            }
        }
        
        // select record to be written
        Record writeRecord;
        bool consumeFlag = false;
        if(fileReadFlag and outputPipeReadFlag){
            if (myCompEng.Compare(fileRecordptr,outputPipeRecordPtr,myPreferencePtr->orderMaker)<=0){
                writeRecord.Consume(fileRecordptr);
                consumeFlag=true;
                getNextFileRecord=true;
            }
            else{
                writeRecord.Consume(outputPipeRecordPtr);
                consumeFlag=true;
                getNextOutputPipeRecord=true;
            }
        }
        else if (fileReadFlag){
            writeRecord.Consume(fileRecordptr);
            consumeFlag=true;
            getNextFileRecord=true;

        }
        else if (outputPipeReadFlag){
            writeRecord.Consume(outputPipeRecordPtr);
            consumeFlag=true;
            getNextOutputPipeRecord=true;
        }
        
        
        if(consumeFlag && !page.Append(&writeRecord)) {
            // Add the page to new File
            cout<<page.getNumRecs()<<"Writing Records"<<endl;
            newFile.AddPage(&page,newFilePageCounter);
            newFilePageCounter++;
            
            // empty page if not consumed
            if(page.getNumRecs() > 0){
                page.EmptyItOut();
            }
            // add again to page
            page.Append(&writeRecord);
        }
    }
    
    if (page.getNumRecs() > 0){
        cout<<page.getNumRecs()<<"Writing Records"<<endl;
        newFile.AddPage(&page,newFilePageCounter);
    }
    
    delete inputPipePtr;
    delete outputPipePtr;
    delete bigQPtr;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    
    myFile.Close();
    newFile.Close();
    
    string oldFileName = fileName.substr(0,fileName.find_last_of('.'))+".bin";
    char* old_f_path = new char[oldFileName.length()+1];
    strcpy(old_f_path, oldFileName.c_str());
    
    if(Utilities::checkfileExist(old_f_path)) {
        if( remove(old_f_path) != 0 )
        cerr<< "Error deleting file" ;
    }
    rename(new_f_path,old_f_path);
    myFile.Open(1,old_f_path);
}
/*-----------------------------------------------------------------------------------*/


/*-----------------------------------------------------------------------------------*/

// stub file .. replace it with your own DBFile.cc
DBFile::DBFile () {
    myFilePtr = NULL;
}

DBFile::~DBFile () {
    delete myFilePtr;
    myFilePtr = NULL;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if (Utilities::checkfileExist(f_path)) {
        cout << "file you are about to create already exists!"<<endl;
        return 0;
    }
    // changing .bin extension to .pref for storing preferences.
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    // loading preferences
    LoadPreference(finalString,f_type);
    
    // check if the file type is correct
    if (f_type == heap){
        myFilePtr = new HeapDBFile(&myPreference);
        myFilePtr->Create((char *)f_path,f_type,startup);
        return 1;
    }
    else if(f_type == sorted){
        myFilePtr = new SortedDBFile(&myPreference);
        myFilePtr->Create((char *)f_path,f_type,startup);
        return 1;
    }
    return 0;
}


int DBFile::Open (const char *f_path) {

    if (!Utilities::checkfileExist(f_path)) {
        cout << "Trying to open a file which is not created yet!"<<endl;
        return 0;
    }

    // changing .bin extension to .pref for storing preferences.
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());

    // loading preferences
    LoadPreference(finalString,undefined);
    
    
    // check if the file type is correct
    if (myPreference.f_type == heap){
        myFilePtr = new HeapDBFile(&myPreference);
    }
    else if(myPreference.f_type == sorted){
        myFilePtr = new SortedDBFile(&myPreference);
    }
    // opening file using given path
    return myFilePtr->Open((char *)f_path);
}

void DBFile::Add (Record &rec) {
    if (myFilePtr!=NULL){
        myFilePtr->Add(rec);
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    if (myFilePtr!=NULL){
           myFilePtr->Load(f_schema,loadpath);
    }
}


void DBFile::MoveFirst () {
    if (myFilePtr!=NULL){
           myFilePtr->MoveFirst();
    }
}

int DBFile::Close () {
    if (myFilePtr!=NULL){
        if (myFilePtr->Close()){
            DumpPreference();
            return 1;

        }
        else{
            return 0;
        };
    }
}


int DBFile::GetNext (Record &fetchme) {
    if (myFilePtr != NULL){
        return myFilePtr->GetNext(fetchme);
    }
    return 0;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if (myFilePtr != NULL){
        return myFilePtr->GetNext(fetchme,cnf,literal);
    }
    return 0;
}

void DBFile::LoadPreference(char * newFilePath,fType f_type) {
    ifstream file;
    if (Utilities::checkfileExist(newFilePath)) {
        file.open(newFilePath,ios::in);
        if(!file){
            cerr<<"Error in opening file..";
            exit(1);
        }
        file.read((char*)&myPreference,sizeof(Preference));
        myPreference.preferenceFilePath = (char*)malloc(strlen(newFilePath) + 1);
        strcpy(myPreference.preferenceFilePath,newFilePath);
        
        myPreference.orderMaker = new OrderMaker();
        file.read((char*)myPreference.orderMaker,sizeof(OrderMaker));
    }
    else {
        myPreference.f_type = f_type;
        myPreference.preferenceFilePath = (char*) malloc(strlen(newFilePath) + 1);
        strcpy(myPreference.preferenceFilePath,newFilePath);
        myPreference.currentPage = 0;
        myPreference.currentRecordPosition = 0;
        myPreference.isPageFull = false;
        myPreference.pageBufferMode = IDLE;
        myPreference.reWriteFlag= false;
        myPreference.allRecordsWritten = true;
        myPreference.orderMaker = NULL;
        myPreference.runLength = 0;
    }
}

void DBFile::DumpPreference(){
    ofstream file;
    file.open(myPreference.preferenceFilePath,ios::out);
    if(!file) {
        cerr<<"Error in opening file for writing.."<<endl;
        exit(1);
    }
    file.write((char*)&myPreference,sizeof(Preference));
    file.write((char*)myPreference.orderMaker,sizeof(OrderMaker));
    file.close();
}
