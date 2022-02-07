#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>

// stub file .. replace it with your own DBFile.cc

// Default Constructor
DBFile::DBFile () 
{
    diskFile = new File();
    currentReadRecord = new Record();
    writePage = new Page();
    readPage = new Page();
    //logfile
}

// Default Destructor
DBFile::~DBFile () 
{
    delete diskFile;
    delete currentReadRecord;
    delete writePage;
    delete readPage;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) 
{
    cout<<"Create\n";
    if (f_type == heap)
    {
        diskFile->Open(0, (char *)f_path);
        writePageIndex = 0;
        readPageIndex = 0;
        dirtyBit = false;
        isCurrentEnd = true;

        MoveFirst();
    }   
    return 1; //success
}

void DBFile::Load (Schema &f_schema, const char *loadpath) 
{
    cout<<"Load\n";
    FILE *bulkFile = fopen(loadpath, "r");
    Record tempRecord;

    while (tempRecord.SuckNextRecord(&f_schema, bulkFile) == 1)
    {
        Add(tempRecord);
    }

    if(dirtyBit == true)
    {
        diskFile->AddPage(writePage, writePageIndex);
    }

    fclose(bulkFile);
}

int DBFile::Open (const char *f_path) 
{
    cout<<"Open\n";
    diskFile->Open(1, (char *)f_path);
    writePageIndex = 0;
    dirtyBit = false;
    isCurrentEnd = false;
    MoveFirst();
    return 1; // success
}

void DBFile::MoveFirst () 
{
    cout<<"Movefirst\n";
    if (diskFile->GetLength() > 0)
    {
        readPage->EmptyItOut();
        diskFile->GetPage(this->readPage,0);
        readPage->GetFirst(currentReadRecord);
    }
    cout << "MoveFirst() length of file is " << diskFile->GetLength() << "\n";

    // should check for dirtyBit?
    // diskFile->GetPage(readPage, 1);
    // readPage->GetFirst(currentRecord);

    // if (dirtyBit == 1) {
    //     diskFile->AddPage(writePage, writePageIndex);
    //     dirtyBit = 0;
    // }
    // writePageIndex = 0;
    // writePage->EmptyItOut();
    // //If DBfile is not empty
    // if (diskFile->GetLength() > 0) {
    //     diskFile->GetPage(writePage, writePageIndex);
    // }
    // //Delete After testing
    // cout << "length of file is " << diskFile->GetLength() << "\n";

    // if (dirtyBit == 1) {
    //     diskFile->AddPage(writePage, writePageIndex);
    //     dirtyBit = 0;
    // }
    // writePageIndex = 0;
    // writePage->EmptyItOut();
    // //If DBfile is not empty
    // if (diskFile->GetLength() > 0) {
    //     diskFile->GetPage(writePage, writePageIndex);
    // }
    // //Delete After testing
    // cout << "length of file is " << diskFile->GetLength() << "\n";
}

int DBFile::Close () 
{
    // if (dirtyBit == true)
    // {
    //     diskFile->AddPage(writePage, writePageIndex);
    //     writePageIndex++;
    // }
    // // TODO: Add a logger function - avoid cout
    // cout<<"\nLength of closed file: "<<diskFile->Close();
    // return 1;

    cout<<"Close\n";   
    if (dirtyBit == 1)
        diskFile->AddPage(writePage, writePageIndex);
    isCurrentEnd = true;
    diskFile->Close();
    cout << "Closing file, length of file is " << diskFile->GetLength() << "Pages" << "\n";
    return 1;
}

void DBFile::Add (Record &rec) 
{
    cout<<"Add\n";
    // works
    dirtyBit = true;

    Record tempWriteRecord;
    tempWriteRecord.Consume(&rec);

    int status = writePage->Append(&tempWriteRecord);

    // handle failure case - i.e. if new record cant fit in current page
    if (status == 0)
    {
        diskFile->AddPage(writePage, writePageIndex);
        writePageIndex++;
        writePage->EmptyItOut();
        writePage->Append(&tempWriteRecord);
    }
}

int DBFile::GetNext (Record &fetchme) 
{
    cout<<"GetNext 1\n";
    if (isCurrentEnd != true)
    {
        fetchme.Copy(currentReadRecord);
        int status = readPage->GetFirst(currentReadRecord);

        // If no data in this page
        if (status == 0)
        {
            readPageIndex++;

            if (readPageIndex >= diskFile->GetLength() - 1)
            {
                isCurrentEnd = true;
            }
            else
            {
                diskFile->GetPage(readPage, readPageIndex);
                readPage->GetFirst(currentReadRecord);
            }
        }
        return 1;
    }
    return 0;

    //If file is writing, then write page into disk based file, redirect page ot first page
    // if (dirtyBit == 1) {
    //     MoveFirst();
    // }
    // //If reach the end of page
    // if (writePage->GetFirst(&fetchme) == 0) {
    //     writePageIndex++;
    //     //If reach the end of file
    //     if (writePageIndex >= diskFile->GetLength() - 1) {
    //         return 0;
    //     }
    //     //Else get next page
    //     writePage->EmptyItOut();
    //     diskFile->GetPage(writePage, writePageIndex);
    //     writePage->GetFirst(&fetchme);
    // }
    // return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    cout<<"GetNext 2\n";
    ComparisonEngine comp;
    //If not reach the end of file
    while (GetNext(fetchme) == 1) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

// void DBFile::InternalLogger (logLevel level, char[] logMessage)
// {
//     // TODO: Add logfile
//     cout<<logMessage;
// }
