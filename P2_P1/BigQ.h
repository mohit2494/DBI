#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Comparison.h"
#include <unordered_map>

using namespace std;

typedef struct{
    int startPage;
    int currentPage;
    int endPage;
    int runId;
} RunFileObject;

typedef struct{
    int runId;
    Record * record;
} QueueObject;

// ------------------------------------------------------------------
typedef struct {
    Pipe * in;
    Pipe * out;
    OrderMaker * sortorder;
    int runlen;
} ThreadData;
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class Run {
    int runLength;
    OrderMaker * sortorder;
    vector <Page *> pages;
    public:
        Run(int runLength);
        Run(int runLength,OrderMaker * sortorder);
        void AddPage(Page *p);
        void sortRunInternalPages();
        bool checkRunFull();
        bool clearPages();
        int getRunSize();
        vector<Page*> getPages();
        void getPages(vector<Page*> * pagevector);
        void sortSinglePage(Page *p);
        bool customRecordComparator(Record &left, Record &right);
};
// ------------------------------------------------------------------


// ------------------------------------------------------------------
class CustomComparator{
    ComparisonEngine myComparisonEngine;
    OrderMaker * myOrderMaker;
public:
    CustomComparator(OrderMaker * sortorder);
    bool operator()( Record* lhs,  Record* rhs);
    bool operator()( QueueObject lhs,  QueueObject rhs);

};
// ------------------------------------------------------------------

// ------------------------------------------------------------------

class RunManager{
    int noOfRuns;
    int runLength;
    int totalPages;
    File file;
    char * f_path;
    unordered_map<int,RunFileObject> runLocation;
public:
    RunManager(int noOfRuns,int runLength,int totalPages,char * f_path);
    void getPages(vector<Page*> * myPageVector);
    bool getNextPageOfRun(Page * page,int runNo);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------

class TournamentTree{
    OrderMaker * myOrderMaker;
    RunManager * myRunManager;
    vector<Page*> myPageVector;
    Page OutputBuffer;
    bool isRunManagerAvailable;
    priority_queue<QueueObject,vector<QueueObject>,CustomComparator> * myQueue;
    void Inititate();
public:
    TournamentTree(Run * run,OrderMaker * sortorder);
    TournamentTree(RunManager * manager,OrderMaker * sortorder);
    void RefillOutputBuffer();
    bool GetSortedPage(Page * *p);
};
// ------------------------------------------------------------------

// ------------------------------------------------------------------
class BigQ {
     ThreadData myThreadData;
     TournamentTree * myTree;
     pthread_t myThread;
     int totalRuns;
     bool isLastRunComplete;
     DBFile myFile;
     void Phase1();
     void Phase2();
public:
    static void* Driver(void*);
    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ ();
};
// ------------------------------------------------------------------

#endif

















//day2


//#ifndef BIGQ_H
//#define BIGQ_H
//#include <pthread.h>
//#include <iostream>
//#include <vector>
//#include <queue>
//#include <algorithm>
//#include "Pipe.h"
//#include "File.h"
//#include "Record.h"
//#include "ComparisonEngine.h"
//#include "DBFile.h"
//#include "Comparison.h"
//using namespace std;
//
//
//// ------------------------------------------------------------------
//typedef struct {
//    Pipe * in;
//    Pipe * out;
//    OrderMaker * sortorder;
//    int runlen;
//} ThreadData;
//// ------------------------------------------------------------------
//
//// ------------------------------------------------------------------
//class Run {
//    int runLength;
//    OrderMaker * sortorder;
//    vector <Page> pages;
//    public:
//        Run(int runLength);
//        Run(int runLength,OrderMaker * sortorder);
//        void AddPage(Page &p);
//        void sortRunInternalPages();
//        bool checkRunFull();
//        bool clearPages();
//        int getRunSize();
//        vector<Page> getPages();
//        void getPages(vector<Page> * pagevector);
//        Page sortSinglePage(Page p);
//        bool customRecordComparator(Record &left, Record &right);
//};
//// ------------------------------------------------------------------
//
//
//// ------------------------------------------------------------------
//class CustomComparator{
//    ComparisonEngine myComparisonEngine;
//    OrderMaker * myOrderMaker;
//public:
//    CustomComparator(OrderMaker * sortorder);
//    bool operator()( Record & lhs,  Record &rhs);
//};
//// ------------------------------------------------------------------
//
//// ------------------------------------------------------------------
//class RunManager{
//    DBFile * myFile;
//    int noOfRuns;
//    int runLength;
//    vector <Page> pages;
//    vector <int> fileDescriptors;
//public:
//    RunManager(int noOfRuns,int runLength,DBFile * file);
//    void getPages(vector<Page> * myPageVector);
//    bool getNextPageOfRun(Page * page,int runNo,int pageOffset);
//};
//// ------------------------------------------------------------------
//
//// ------------------------------------------------------------------
//class TournamentTree{
//    OrderMaker * myOrderMaker;
//    RunManager * myRunManager;
//    vector<Page> myPageVector;
//    Page OutputBuffer;
//    bool isRunManagerAvailable;
//    priority_queue<Record,vector<Record>,CustomComparator> * myQueue;
//    void Inititate();
//public:
//    TournamentTree(Run * run,OrderMaker * sortorder);
//    TournamentTree(RunManager * manager,OrderMaker * sortorder);
//    bool GetSortedPage(Page &p);
//};
//// ------------------------------------------------------------------
//
//// ------------------------------------------------------------------
//class BigQ {
//     ThreadData myThreadData;
//     TournamentTree * myTree;
//     pthread_t myThread;
//     int totalRuns;
//     bool isLastRunComplete;
//     DBFile myFile;
//     void Phase1();
//     void Phase2();
//public:
//    static void* Driver(void*);
//    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
//    ~BigQ ();
//};
//// ------------------------------------------------------------------
//
//#endif
//
//
//
//







// my code startes here

//#ifndef BIGQ_H
//#define BIGQ_H
//#include <pthread.h>
//#include <iostream>
//#include "Pipe.h"
//#include "File.h"
//#include "Record.h"
//#include <vector>
//#include <queue>
//#include "ComparisonEngine.h"
//#include "DBFile.h"
//using namespace std;
//
//
//typedef struct {
//    Pipe * in;
//    Pipe * out;
//    OrderMaker * sortorder;
//    int runlen;
//} ThreadData;
//
//
//class TreeComparator{
//    ComparisonEngine myComparisonEngine;
//    OrderMaker * myOrderMaker;
//public:
//    TreeComparator(OrderMaker &sortorder);
//    bool operator()(const Record & lhs, const Record &rhs);
//};
//
//class RunManager{
//    DBFile * myFile;
//    int noOfRuns;
//    int runLength;
//    vector <Page> pages;
//    vector <int> fileDescriptors;
//public:
//    vector<Page *> getPagesForSorting();
//    Page * getNextPageOfRun(int runNo,int pageOffset);
//};
//
//
//class Run {
//    int runLength;
//    vector <Page> pages;
//    public:
//        Run(int runLength);
//        void AddPage(Page p);
//        void sortRun();
//        bool checkRunFull();
//        bool clearPages();
//        int getRunSize();
//        vector<Page> getPages();
//};
//
//
//class TournamentTree{
//    OrderMaker * myOrderMaker;
//    RunManager * myRunManager;
//    vector<Page> myPageVector;
//    Page OutputBuffer;
//    bool isRunManagerAvailable;
//    priority_queue<Record,vector<Record>,TreeComparator> * myQueue;
//    void Inititate();
//public:
//    TournamentTree(OrderMaker &sortorder,RunManager &manager);
//    bool GetSortedPage(Page &p);
//};
//
//
//
//
//class BigQ {
//    ThreadData myThreadData;
//    TournamentTree * myTree;
//    pthread_t myThread;
//    int totalRuns;
//    bool isLastRunComplete;
//    DBFile myFile;
//    void Inititate();
//    void Phase1();
//    void Phase2();
//
//public:
//  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
//	~BigQ ();
//  static void* Driver(void*);
//};
//
//#endif
