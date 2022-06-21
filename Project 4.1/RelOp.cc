#include "RelOp.h"
#include <iostream>
#include "BigQ.h"

typedef struct
{
    DBFile &inFile;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} WorkerArg1;

void *workerMain1(void *arg)
{
    WorkerArg1 *workerArg = (WorkerArg1 *)arg;
    Record rec;

    while (workerArg->inFile.GetNext(rec, workerArg->selOp, workerArg->literal))
    {
        workerArg->outPipe.Insert(&rec);
    }

    workerArg->outPipe.ShutDown();

    return NULL;
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
    WorkerArg1 *workerArg = new WorkerArg1{inFile, outPipe, selOp, literal};
    pthread_create(&worker, NULL, workerMain1, (void *)workerArg);
}

void SelectFile::WaitUntilDone()
{
    pthread_join(worker, NULL);
}

void SelectFile::Use_n_Pages(int runlen)
{
}

typedef struct
{
    Pipe &inPipe;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} WorkerArg2;

void *workerMain2(void *arg)
{
    ComparisonEngine comp;
    WorkerArg2 *workerArg = (WorkerArg2 *)arg;
    Record rec;

    while (workerArg->inPipe.Remove(&rec))
    {
        if (comp.Compare(&rec, &workerArg->literal, &workerArg->selOp))
        {
            workerArg->outPipe.Insert(&rec);
        }
    }

    workerArg->outPipe.ShutDown();

    return NULL;
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
    WorkerArg2 *workerArg = new WorkerArg2{inPipe, outPipe, selOp, literal};
    pthread_create(&worker, NULL, workerMain2, (void *)workerArg);
}

void SelectPipe::WaitUntilDone()
{
    pthread_join(worker, NULL);
}

void SelectPipe::Use_n_Pages(int runlen)
{
}

typedef struct
{
    Pipe &inPipe;
    Pipe &outPipe;
    Function &computeMe;
} WorkerArg3;

void *workerMain3(void *arg)
{
    int intSum = 0, intResult = 0;
    double doubleSum = 0.0, doubleResult = 0.0;
    ComparisonEngine comp;
    WorkerArg3 *workerArg = (WorkerArg3 *)arg;
    Record rec;
    Type t;

    while (workerArg->inPipe.Remove(&rec))
    {
        t = workerArg->computeMe.Apply(rec, intResult, doubleResult);
        if (t == Int)
        {
            intSum += intResult;
        }
        else
        {
            doubleSum += doubleResult;
        }
    }

    Attribute DA = {"SUM", t};
    Schema out_sch("out_sch", 1, &DA);

    Record res;
    char charsRes[100];

    if (t == Int)
    {
        sprintf(charsRes, "%d|", intSum);
    }
    else
    {
        sprintf(charsRes, "%lf|", doubleSum);
    }

    res.ComposeRecord(&out_sch, charsRes);
    workerArg->outPipe.Insert(&res);
    workerArg->outPipe.ShutDown();

    return NULL;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
    WorkerArg3 *workerArg = new WorkerArg3{inPipe, outPipe, computeMe};
    pthread_create(&worker, NULL, workerMain3, (void *)workerArg);
}

void Sum::WaitUntilDone()
{
    pthread_join(worker, NULL);
}

void Sum::Use_n_Pages(int n)
{
}


typedef struct
{
    Pipe &inPipe;
    Pipe &outPipe;
    OrderMaker &groupAtts;
    Function &computeMe;
    int use_n_pages;
} WorkerArg4;

void *workerMain4(void *arg)
{
    WorkerArg4 *workerArg = (WorkerArg4 *)arg;
    Pipe sorted(100);
    BigQ bigQ(workerArg->inPipe, sorted, workerArg->groupAtts, workerArg->use_n_pages);
    int intRes = 0, intSum = 0;
    double doubleRes = 0.0, doubleSum = 0.0;
    
    ComparisonEngine cmp;
    Record prev_rec;
    Record cur_rec;
    Type t;
    Attribute DA = {"SUM", t};
    Schema out_sch("out_sch", 1, &DA);
    bool firstTime = true;

    while (sorted.Remove(&cur_rec))
    {
        if (!firstTime && cmp.Compare(&cur_rec, &prev_rec, &workerArg->groupAtts) != 0)
        {
            cout << "===" << endl;
            Record res;
            char charsRes[100];

            if (t == Int)
            {
                sprintf(charsRes, "%d|", intSum);
            }
            else
            {
                sprintf(charsRes, "%lf|", doubleSum);
            }
            
            res.ComposeRecord(&out_sch, charsRes);
            workerArg->outPipe.Insert(&res);

            intSum = 0;
            doubleSum = 0.0;
        }

        firstTime = false;
        t = workerArg->computeMe.Apply(cur_rec, intRes, doubleRes);
        if (t == Int)
        {
            intSum += intRes;
        }
        else
        {
            doubleSum += doubleRes;
        }
        prev_rec.Copy(&cur_rec);
    }

    Record res;

    char charsRes[100];

    if (t == Int)
    {
        sprintf(charsRes, "%d|", intSum);
    }
    else
    {
        sprintf(charsRes, "%lf|", doubleSum);
    }

    res.ComposeRecord(&out_sch, charsRes);
    workerArg->outPipe.Insert(&res);
    workerArg->outPipe.ShutDown();

    return NULL;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
    WorkerArg4 *workerArg = new WorkerArg4{inPipe, outPipe, groupAtts, computeMe, use_n_pages};
    pthread_create(&worker, NULL, workerMain4, (void *)workerArg);
}

void GroupBy::WaitUntilDone()
{
    pthread_join(worker, NULL);
}

void GroupBy::Use_n_Pages(int n)
{
    use_n_pages = n;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
    ProjectArg *projectArg = new ProjectArg;
    projectArg->inPipe = &inPipe;
    projectArg->outPipe = &outPipe;
    projectArg->keepMe = keepMe;
    projectArg->numAttsInput = numAttsInput;
    projectArg->numAttsOutput = numAttsOutput;
    pthread_create(&workerThread, NULL, ProjectWorker, (void *)projectArg);
}

void *ProjectWorker(void *arg)
{
    ProjectArg *projectArg = (ProjectArg *)arg;
    Record record;

    while (projectArg->inPipe->Remove(&record) == 1)
    {
        Record *tempRecord = new Record;
        tempRecord->Consume(&record);
        tempRecord->Project(projectArg->keepMe, projectArg->numAttsOutput, projectArg->numAttsInput);
        projectArg->outPipe->Insert(tempRecord);
    }

    projectArg->outPipe->ShutDown();

    return NULL;
}

void Project::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void Project::Use_n_Pages(int n)
{
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
    DuplicateRemovalArg *duplicateRemovalArg = new DuplicateRemovalArg;
    duplicateRemovalArg->inPipe = &inPipe;
    duplicateRemovalArg->outPipe = &outPipe;
    OrderMaker *order = new OrderMaker(&mySchema);
    duplicateRemovalArg->order = order;

    if (this->runLen <= 0)
    {
        duplicateRemovalArg->runLen = 8;
    }
    else
    {
        duplicateRemovalArg->runLen = this->runLen;
    }

    pthread_create(&workerThread, NULL, DuplicateRemovalWorker, (void *)duplicateRemovalArg);
}

void *DuplicateRemovalWorker(void *arg)
{
    DuplicateRemovalArg *duplicateRemovalArg = (DuplicateRemovalArg *)arg;
    ComparisonEngine comparisonEngine;
    Record cur, last;
    Pipe *sortedPipe = new Pipe(1000);

    BigQ *bq = new BigQ(*(duplicateRemovalArg->inPipe), *sortedPipe, *(duplicateRemovalArg->order), duplicateRemovalArg->runLen);
    sortedPipe->Remove(&last);
    Schema schema("catalog", "partsupp");

    while (sortedPipe->Remove(&cur) == 1)
    {
        if (comparisonEngine.Compare(&last, &cur, duplicateRemovalArg->order) != 0)
        {
            Record *temp_rec = new Record;
            temp_rec->Consume(&last);
            duplicateRemovalArg->outPipe->Insert(temp_rec);
            last.Consume(&cur);
        }
    }

    duplicateRemovalArg->outPipe->Insert(&last);
    duplicateRemovalArg->outPipe->ShutDown();

    return NULL;
}

void DuplicateRemoval::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n)
{
    this->runLen = n;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
    WriteOutArg *writeout_arg = new WriteOutArg;
    writeout_arg->inPipe = &inPipe;
    writeout_arg->outFile = outFile;
    writeout_arg->schema = &mySchema;
    pthread_create(&workerThread, NULL, WriteOutWorker, (void *)writeout_arg);
}

void *WriteOutWorker(void *arg)
{
    WriteOutArg *writeOutArg = (WriteOutArg *)arg;
    Record cur;

    while (writeOutArg->inPipe->Remove(&cur) == 1)
    {
        int numOfAtts = writeOutArg->schema->GetNumAtts();
        Attribute *attribute = writeOutArg->schema->GetAtts();

        for (int i = 0; i < numOfAtts; i++)
        {
            fprintf(writeOutArg->outFile, "%s:", attribute[i].name);

            int pointer = ((int *)cur.bits)[i + 1];

            fprintf(writeOutArg->outFile, "[");

            if (attribute[i].myType == Int)
            {
                int *writeOutInt = (int *)&(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%d", *writeOutInt);
            }
            else if (attribute[i].myType == Double)
            {
                double *writeOutDouble = (double *)&(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%f", *writeOutDouble);
            }
            else if (attribute[i].myType == String)
            {
                char *writeOutString = (char *)&(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%s", writeOutString);
            }

            fprintf(writeOutArg->outFile, "]");

            if (i != numOfAtts - 1)
                fprintf(writeOutArg->outFile, ", ");
        }
        fprintf(writeOutArg->outFile, "\n");
    }
    return NULL;
}

void WriteOut::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void WriteOut::Use_n_Pages(int n)
{
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
    JoinArg *joinArg = new JoinArg;
    joinArg->inPipeL = &inPipeL;
    joinArg->inPipeR = &inPipeR;
    joinArg->outPipe = &outPipe;
    joinArg->selOp = &selOp;
    joinArg->literal = &literal;

    if (this->runLen <= 0)
    {
        joinArg->runLen = 8;
    }
    else
    {
        joinArg->runLen = this->runLen;
    }

    pthread_create(&workerThread, NULL, JoinWorker, (void *)joinArg);
}

void *JoinWorker(void *arg)
{
    JoinArg *joinArg = (JoinArg *)arg;
    OrderMaker left_order, right_order;
    joinArg->selOp->GetSortOrders(left_order, right_order);

    if (left_order.numAtts > 0 && right_order.numAtts > 0)
    {
        cout << "Enter sort merge " << endl;
        JoinWorker_Merge(joinArg, &left_order, &right_order);
    }
    else
    {
        cout << "BlockNestJoin" << endl;
        JoinWorker_BlockNested(joinArg);
    }

    joinArg->outPipe->ShutDown();

    return NULL;
}

void JoinWorker_AddMergedRecord(Record *leftRecord, Record *rightRecord, Pipe *pipe)
{
    int att_count_left = ((((int *)leftRecord->bits)[1]) / sizeof(int)) - 1;
    int att_count_right = ((((int *)rightRecord->bits)[1]) / sizeof(int)) - 1;
    int *att_to_keep = new int[att_count_left + att_count_right];

    for (int i = 0; i < att_count_left; i++)
        att_to_keep[i] = i;

    for (int i = att_count_left; i < att_count_left + att_count_right; i++)
        att_to_keep[i] = i - att_count_left;

    Record merged_rec;

    merged_rec.MergeRecords(leftRecord, rightRecord, att_count_left, att_count_right, att_to_keep, att_count_left + att_count_right, att_count_left);
    pipe->Insert(&merged_rec);
}

void JoinWorker_Merge(JoinArg *joinArg, OrderMaker *leftOrder, OrderMaker *rightOrder)
{
    Pipe *sortedLeftPipe = new Pipe(1000);
    Pipe *sortedRightPipe = new Pipe(1000);
    
    BigQ *tempL = new BigQ(*(joinArg->inPipeL), *sortedLeftPipe, *leftOrder, joinArg->runLen);
    BigQ *tempR = new BigQ(*(joinArg->inPipeR), *sortedRightPipe, *rightOrder, joinArg->runLen);

    Record left_rec;
    Record right_rec;
    bool isFinish = false;

    if (sortedLeftPipe->Remove(&left_rec) == 0)
        isFinish = true;

    if (sortedRightPipe->Remove(&right_rec) == 0)
        isFinish = true;

    ComparisonEngine comparisonEngine;

    while (!isFinish)
    {
        int compareRes = comparisonEngine.Compare(&left_rec, leftOrder, &right_rec, rightOrder);

        if (compareRes == 0)
        {
            vector<Record *> vl;
            vector<Record *> vr;

            while (true)
            {
                Record *prev_left_rec = new Record;
                prev_left_rec->Consume(&left_rec);
                vl.push_back(prev_left_rec);
                if (sortedLeftPipe->Remove(&left_rec) == 0)
                {
                    isFinish = true;
                    break;
                }
                if (comparisonEngine.Compare(&left_rec, prev_left_rec, leftOrder) != 0)
                {
                    break;
                }
            }

            while (true)
            {
                Record *old_right_rec = new Record;
                old_right_rec->Consume(&right_rec);
                vr.push_back(old_right_rec);

                if (sortedRightPipe->Remove(&right_rec) == 0)
                {
                    isFinish = true;
                    break;
                }

                if (comparisonEngine.Compare(&right_rec, old_right_rec, rightOrder) != 0)
                {
                    break;
                }
            }

            for (int i = 0; i < vl.size(); i++)
            {
                for (int j = 0; j < vr.size(); j++)
                {
                    JoinWorker_AddMergedRecord(vl[i], vr[j], joinArg->outPipe);
                }
            }
            vl.clear();
            vr.clear();
        }
        else if (compareRes > 0)
        {
            if (sortedRightPipe->Remove(&right_rec) == 0)
                isFinish = true;
        }
        else
        {
            if (sortedLeftPipe->Remove(&left_rec) == 0)
                isFinish = true;
        }
    }

    cout << "Finish read fron sorted pipe" << endl;
    while (sortedLeftPipe->Remove(&left_rec) == 1)
        ;
    while (sortedRightPipe->Remove(&right_rec) == 1)
        ;
}

void JoinWorker_BlockNested(JoinArg *joinArg)
{
    DBFile temp_file;
    char *fileName = new char[100];
    sprintf(fileName, "BlockNestedTemp%d.bin", pthread_self());

    temp_file.Create(fileName, heap, NULL);
    temp_file.Open(fileName);
    Record rec;

    while (joinArg->inPipeL->Remove(&rec) == 1)
        temp_file.Add(rec);

    Record rec1;
    Record rec2;
    ComparisonEngine comparisonEngine;

    while (joinArg->inPipeR->Remove(&rec1) == 1)
    {
        temp_file.MoveFirst();
        while (temp_file.GetNext(rec) == 1)
        {
            if (comparisonEngine.Compare(&rec1, &rec2, joinArg->literal, joinArg->selOp))
            {
                JoinWorker_AddMergedRecord(&rec1, &rec2, joinArg->outPipe);
            }
        }
    }
}

void Join::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void Join::Use_n_Pages(int n)
{
    this->runLen = n;
}
