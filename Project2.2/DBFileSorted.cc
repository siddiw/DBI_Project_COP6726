#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileSorted.h"
#include "Defs.h"
#include <chrono>
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>

DBFileSorted::DBFileSorted () 
{
    isWriting = 0;
    pageIndex = 0;
}

int DBFileSorted::Create (char *f_path, fType f_type, void *startup) 
{
    cout<<"DBFileSorted Create starts"<<endl;
    diskFile.Open(0, const_cast<char *>(f_path));
    out_path = f_path;
    pageIndex = 0;
    isWriting = 0;
    MoveFirst();

    cout<<"DBFileSorted Create ends" << endl;
    return 1;
}


void DBFileSorted::Load (Schema &f_schema, char *loadpath) 
{
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    ComparisonEngine comp;

    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) 
    {
        this->Add(temp);
    }

    fclose(tableFile);
}

int DBFileSorted::Open (char *f_path) 
{
    diskFile.Open(1, const_cast<char *>(f_path));
    pageIndex = 0;
    out_path = f_path;
    isWriting = 0;
    MoveFirst();
    return 1;
}

void DBFileSorted::MoveFirst () 
{
    readingMode();
    pageIndex = 0;
    bufferPage.EmptyItOut();

    if (diskFile.GetLength() > 0) 
    {
        diskFile.GetPage(&bufferPage, pageIndex);
    }
}

int DBFileSorted::Close () 
{
    readingMode();
    bufferPage.EmptyItOut();
    diskFile.Close();
    if(in!= nullptr)delete in;
    if(out!= nullptr)delete out;
    return 1;
}

void DBFileSorted::Add (Record &rec)
{
    writingMode();
    in->Insert(&rec);
}

int DBFileSorted::GetNext (Record &fetchme) 
{
    readingMode();

    if (bufferPage.GetFirst(&fetchme) == 0) 
    {
        pageIndex++;

        if (pageIndex >= diskFile.GetLength() - 1) 
        {
            return 0;
        }

        bufferPage.EmptyItOut();
        diskFile.GetPage(&bufferPage, pageIndex);
        bufferPage.GetFirst(&fetchme);
    }

    return 1;
}

int DBFileSorted::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
    if(!calculatedBound) 
    {
        calculatedBound = 1;
        set<int> attSet;
        for (int i = 0; i < orderMaker->numAtts; i++)
        {
            attSet.insert(orderMaker->whichAtts[i]);
        }

        int global_lower = 0, global_higher = diskFile.GetLength() - 2;

        cout << "original LB: " << global_lower << endl;
        cout << "original HB: " << global_higher << endl;

        for (int i = 0; i < cnf.numAnds; i++) 
        {
            for (int j = 0; j < cnf.orLens[i]; j++) 
            {
                if (attSet.find(cnf.orList[i][j].whichAtt1) == attSet.end())
                    continue;

                if (cnf.orList[i][j].op == LessThan)
                {
                    int left = 0, right = diskFile.GetLength() - 2;
                    Record rec;

                    while (left < right)
                    {
                        int mid = left + (right - left + 1) / 2;
                        diskFile.GetPage(&bufferPage, mid);
                        bufferPage.GetFirst(&rec);
                        int result = Run(&rec, &literal, &cnf.orList[i][j]);

                        if (result != 0) 
                        {
                            left = mid;
                        } 
                        else
                        {
                            right = mid - 1;
                        }
                    }

                    global_higher = min(global_higher, right);
                }
                else if (cnf.orList[i][j].op == GreaterThan) 
                {
                    int left = 0, right = diskFile.GetLength() - 2;
                    Record rec;

                    while (left < right) 
                    {
                        int mid = left + (right - left) / 2;
                        diskFile.GetPage(&bufferPage, mid);
                        bufferPage.GetFirst(&rec);
                        int result = Run(&rec, &literal, &cnf.orList[i][j]);

                        if (result != 0) 
                        {
                            right = mid;
                        } 
                        else 
                        {
                            left = mid + 1;
                        }
                    }

                    global_lower = max(global_lower, left);
                }
            }
        }

        global_lower = global_lower - 1;
        global_lower = max(0, global_lower);

        cout << "updated LB by Binary Search: " << global_lower << endl;
        cout << "updated HB by Binary Search: " << global_higher << endl;

        lowerBound = global_lower;
        higherBound = global_higher;
        pageIndex = global_lower;
    }

    ComparisonEngine comp;

    while (GetNext(fetchme) == 1) 
    {
        if (pageIndex > higherBound)
            return 0;

        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }

    return 0;
}

void DBFileSorted::writingMode()
{
    calculatedBound = 0;

    if(isWriting==0) 
    {
        cout << "begin writeMode " << endl;
        isWriting = 1;
    
        WorkerArgs *workerArg = new WorkerArgs;
        workerArg->pipeIn = in;
        workerArg->pipeOut = out;
        workerArg->order = orderMaker;
        workerArg->runLength = runLength;

        thread = new pthread_t();
        pthread_create(thread, NULL, workerFunction, (void *) workerArg);
        cout << "end writeMode " << endl;
    }
}

void DBFileSorted::readingMode()
{
    if(isWriting==1)
    {
        calculatedBound = 0;
        isWriting = 0;
        in->ShutDown();

        if(thread!= nullptr) 
        {
            pthread_join (*thread, NULL);
            delete thread;
        }

        char* f_merge = "tempMergeddFile.bin";
        char* f_dif = "tempDiffFile.bin";

        DBFile mergedFile;
        mergedFile.Create(f_merge, heap, nullptr);

        DBFile difFile;
        difFile.Open(f_dif);

        this->MoveFirst();
        Record rec1, rec2;
        ComparisonEngine comparisonEngine;

        int st1 = difFile.GetNext(rec1);
        int st2 = this->GetNext(rec2);

        while(st1 && st2)
        {
            if (comparisonEngine.Compare(&rec1, &rec2, orderMaker) < 0)
            {
                mergedFile.Add(rec1);
                st1 = difFile.GetNext(rec1);
            }
            else
            {
                mergedFile.Add(rec2);
                st2 = this->GetNext(rec2);
            }
        }

        while(st1)
        {
            mergedFile.Add(rec1);
            st1 = difFile.GetNext(rec1);
        }

        while(st2)
        {
            mergedFile.Add(rec2);
            st2 = this->GetNext(rec2);
        }

        difFile.Close();
        mergedFile.Close();
        diskFile.Close();
        remove(f_dif);
        remove(out_path);
        rename(f_merge, out_path);
    }
}

int DBFileSorted :: Run (Record *left, Record *literal, Comparison *c) 
{
    char *val1, *val2;
    char *left_bits = left->GetBits();
    char *lit_bits = literal->GetBits();

    if (c->operand1 == Left) 
    {
        val1 = left_bits + ((int *) left_bits)[c->whichAtt1 + 1];
    } 
    else 
    {
        val1 = lit_bits + ((int *) lit_bits)[c->whichAtt1 + 1];
    }

    if (c->operand2 == Left) 
    {
        val2 = left_bits + ((int *) left_bits)[c->whichAtt2 + 1];
    } 
    else 
    {
        val2 = lit_bits + ((int *) lit_bits)[c->whichAtt2 + 1];
    }


    int val1Int, val2Int, tempResult;
    double val1Double, val2Double;

    switch (c->attType) 
    {
        case Int:
        
            val1Int = *((int *) val1);
            val2Int = *((int *) val2);

            if(c->op == GreaterThan)
            {
                return (val1Int > val2Int);
            }
            else if(c->op == LessThan)
            {
                return (val1Int < val2Int);
            }
            else
            {
                return (val1Int == val2Int);
            }

            break;

        case Double:

            val1Double = *((double *) val1);
            val2Double = *((double *) val2);

            if(c->op == GreaterThan)
            {
                return (val1Double > val2Double);
            }
            else if(c->op == LessThan)
            {
                return (val1Double < val2Double);
            }
            else
            {
                return (val1Double == val2Double);
            }

            break;

        default:
            tempResult = strcmp(val1, val2);

            if(c->op == GreaterThan)
            {
                return tempResult > 0;
            }
            else if(c->op == LessThan)
            {
                return tempResult < 0;
            }
            else
            {
                return tempResult == 0;
            }

            break;
    }
}