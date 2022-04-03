#include "BigQ.h"

void* workerFunction(void* arg) 
{
    WorkerArgs* workerArgs = (WorkerArgs*) arg;
    priority_queue<Run*, vector<Run*>, RunChecker> runQueue(workerArgs->order);
    priority_queue<Record*, vector<Record*>, RecordsChecker> recordQueue (workerArgs->order);
    vector<Record* > recBuff;
    Record currentRecord;
    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;

    File file;
    char* fileName = "tmp.bin";
    file.Open(0, fileName);

    // To fetch all records from input pipe
    while (workerArgs->pipeIn->Remove(&currentRecord) == 1)
    {
        Record* tempRecord = new Record;
        tempRecord->Copy(&currentRecord);

        //Add to next page if current page is full
        if (bufferPage.Append(&currentRecord) == 0)
        {
            pageCounter++;
            bufferPage.EmptyItOut();

            //Add to next run if current run is full
            if (pageCounter == workerArgs->runLength)
            {
                recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
                recordQueue = priority_queue<Record*, vector<Record*>, RecordsChecker> (workerArgs->order);
                pageCounter = 0;
            }

            bufferPage.Append(&currentRecord);
        }

        recordQueue.push(tempRecord);
    }

    // Last run handling
    if (!recordQueue.empty())
    {
        recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
        recordQueue = priority_queue<Record*, vector<Record*>, RecordsChecker> (workerArgs->order);
    }

    // Merge all
    while (!runQueue.empty())
    {
        Run* currRun = runQueue.top();
        runQueue.pop();
        workerArgs->pipeOut->Insert(currRun->topRecord);
        if (currRun->UpdateTopRecord() == 1) 
        {
            runQueue.push(currRun);
        }
    }
    file.Close();
    workerArgs->pipeOut->ShutDown();
    return NULL;
}

// Put records into a run
void* recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordsChecker>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunChecker>& runQueue, File& file, Page& bufferPage, int& pageIndex)
{
    bufferPage.EmptyItOut();
    int startIndex = pageIndex;
    while (!recordQueue.empty()) 
    {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(recordQueue.top());
        recordQueue.pop();
        if (bufferPage.Append(tmpRecord) == 0) 
        {
            file.AddPage(&bufferPage, pageIndex++);
            bufferPage.EmptyItOut();
            bufferPage.Append(tmpRecord);
        } 
    }

    file.AddPage(&bufferPage, pageIndex++);
    bufferPage.EmptyItOut();

    Run* newRun = new Run(&file, startIndex, pageIndex - startIndex);
    runQueue.push(newRun);

    return NULL;
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
    pthread_t worker;
    WorkerArgs* workerArg = new WorkerArgs;
    workerArg->pipeIn = &in;
    workerArg->pipeOut = &out;
    workerArg->order = &sortorder;
    workerArg->runLength = runlen;

    pthread_create(&worker, NULL, workerFunction, (void*) workerArg);
    pthread_join(worker, NULL);
	out.ShutDown ();
}

BigQ::~BigQ () 
{

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
    // if bufferPage is full
    if (bufferPage.GetFirst(topRecord) == 0) 
    {
        // if reach the last page
        curPage++;
        if (curPage == startPage + runLength) 
        {
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


