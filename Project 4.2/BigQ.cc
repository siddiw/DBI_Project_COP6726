#include "BigQ.h"
#include "DBFile.h"

void* workerFunction(void* arg) {
    ((BigQ*) arg)->BigQMain();
    return NULL;
}



void BigQ::BigQMain() {
    priority_queue<Run*, vector<Run*>, RunChecker> runQueue(this->order);
    priority_queue<Record*, vector<Record*>, RecordsChecker> recordQueue (this->order);
    vector<Record* > recBuff;
    Record curRecord;

    //Set disk based file for sorting
    File file;
    char* fileName = new char[100];
    sprintf(fileName, "%d.bin", pthread_self());
    file.Open(0, fileName);
    //Buffer page used for disk based file
    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;
    //Retrieve all records from input pipe
    while (this->in->Remove(&curRecord) == 1) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(&curRecord);
        //Add to another page if current page is full
        if (bufferPage.Append(&curRecord) == 0) {
            pageCounter++;
            bufferPage.EmptyItOut();

            //Add to another run if current run is full
            if (pageCounter == this->runlen) {
                recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
                recordQueue = priority_queue<Record*, vector<Record*>, RecordsChecker> (this->order);
                pageCounter = 0;
            }

            bufferPage.Append(&curRecord);
        }

        recordQueue.push(tmpRecord);
    }
    // Handle the last run
    if (!recordQueue.empty()) {
        recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
        recordQueue = priority_queue<Record*, vector<Record*>, RecordsChecker> (this->order);
    }
    // Merge for all runs
    // DBFile dbFileHeap;
    // dbFileHeap.Create("tempDifFile.bin", heap, nullptr);
    Run* run;
    Schema schema ("catalog", "supplier");
//    cout << "2" << endl;
    while (!runQueue.empty()) {
        run = runQueue.top();
        runQueue.pop();
        // dbFileHeap.Add(*(run->topRecord));
        Record* waitToInsert = new Record();
        waitToInsert->Copy(run->topRecord);
        // waitToInsert->Print(&schema);
        this->out->Insert(waitToInsert);
        if (run->UpdateTopRecord() == 1) {
            runQueue.push(run);
        }
    }
//    cout << "3" << endl;
    // dbFileHeap.Close();
    file.Close();
    this->out->ShutDown();
    remove(fileName);
}


// void* workerFunction(void* arg) 
// {
//     cout<<"workerFunction starts" << endl;
//     WorkerArgs* workerArg = (WorkerArgs*) arg;
//     priority_queue<Run*, vector<Run*>, RunChecker> runQueue(workerArg->order);
//     priority_queue<Record*, vector<Record*>, RecordsChecker> recordQueue (workerArg->order);

//     vector<Record*> recBuff;
//     Page bufferPage;
//     Record currentRecord;

//     int pageIdx = 0;
//     int pageCounter = 0;

//     File file;
//     char* fileName = "tmp.bin";
//     file.Open(0, fileName); 

//     while (workerArg->pipeIn->Remove(&currentRecord) == 1) 
//     {
//         Record* tmpRecord = new Record;
//         tmpRecord->Copy(&currentRecord);

//         if (bufferPage.Append(&currentRecord) == 0) 
//         {
//             pageCounter++;
//             bufferPage.EmptyItOut();

//             if (pageCounter == workerArg->runLength) 
//             {
//                 recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIdx);
//                 recordQueue = priority_queue<Record*, vector<Record*>, RecordsChecker> (workerArg->order);
//                 pageCounter = 0;
//             }

//             bufferPage.Append(&currentRecord);
//         }

//         recordQueue.push(tmpRecord);
//     }

//     if (!recordQueue.empty()) 
//     {
//         recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIdx);
//         recordQueue = priority_queue<Record*, vector<Record*>, RecordsChecker> (workerArg->order);
//     }

//     DBFile dbFileHeap;
//     cout<< "1" << endl;
//     dbFileHeap.Create("tempDiffFile.bin", heap, nullptr);

//     //Record rec;

//     while (!runQueue.empty()) {
//         Run* run = runQueue.top();
//         runQueue.pop();
//         dbFileHeap.Add(*(run->topRecord));
//         if (run->UpdateTopRecord() == 1) {
//             runQueue.push(run);
//         }
//     }
//     dbFileHeap.Close();
//     file.Close();
//     workerArg->pipeOut->ShutDown();
//     cout<<"end workerFunction" << endl;
//     return NULL;
// }

void BigQ::recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordsChecker>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunChecker>& runQueue, File& file, Page& bufferPage, int& pageIdx) 
{

    bufferPage.EmptyItOut();
    int startIndex = pageIdx;
    while (!recordQueue.empty()) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(recordQueue.top());
        recordQueue.pop();
        if (bufferPage.Append(tmpRecord) == 0) {
            file.AddPage(&bufferPage, pageIdx++);
            bufferPage.EmptyItOut();
            bufferPage.Append(tmpRecord);
        }
    }
    file.AddPage(&bufferPage, pageIdx++);
    bufferPage.EmptyItOut();
    Run* run = new Run(&file, startIndex, pageIdx - startIndex);
    runQueue.push(run);
    //return NULL;
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
    // cout<< "begin BigQ" << endl;
    // pthread_t worker;

    // WorkerArgs* workerArg = new WorkerArgs;
    // workerArg->pipeIn = &in;
    // workerArg->pipeOut = &out;
    // workerArg->order = &sortorder;
    // workerArg->runLength = runlen;
    // pthread_create(&worker, NULL, workerFunction, (void*) workerArg);
    // cout<< "end BigQ" << endl;

    pthread_t worker;
    //Construct arguement used for worker thread
    // this* this = new this;
    this->in = &in;
    this->out = &out;
    this->order = &sortorder;
    this->runlen = runlen;
    pthread_create(&worker, NULL, workerFunction, (void*) this);
    // pthread_join(worker, NULL);



}

BigQ::~BigQ () {

}

Run::Run(File* file, int start, int length) 
{
    fileBase = file;
    startPage = start;
    runLength = length;
    curPage = start;
    fileBase->GetPage(&bufferPage, startPage);
    topRecord = new Record;
    UpdateTopRecord();
}

int Run::UpdateTopRecord() 
{
    if (bufferPage.GetFirst(topRecord) == 0) {
        curPage++;
        if (curPage == startPage + runLength) {
            return 0;
        }
        bufferPage.EmptyItOut();
        fileBase->GetPage(&bufferPage, curPage);
        bufferPage.GetFirst(topRecord);
    }
    return 1;
}

RecordsChecker::RecordsChecker(OrderMaker* orderMaker) 
{
    order = orderMaker;
}

bool RecordsChecker::operator () (Record* left, Record* right) 
{
    ComparisonEngine comparisonEngine;
    if (comparisonEngine.Compare(left, right, order) >= 0)
        return true;
    return false;
}

RunChecker::RunChecker(OrderMaker* orderMaker) 
{
    order = orderMaker;
}

bool RunChecker::operator () (Run* left, Run* right)
{
    ComparisonEngine comparisonEngine;
    if (comparisonEngine.Compare(left->topRecord, right->topRecord, order) >= 0)
        return true;
    return false;
}


