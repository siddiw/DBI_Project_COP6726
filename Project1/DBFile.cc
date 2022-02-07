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
    currentRecord = new Record();
    writePage = new Page();
    readPage = new Page();
    //logfile
}

// Default Destructor
DBFile::~DBFile () 
{
    delete diskFile;
    delete currentRecord;
    delete writePage;
    delete readPage;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) 
{
    if (f_type == heap)
    {
        diskFile->Open(0, (char *)f_path);
        writePageIndex = 0;
        readPageIndex = 0;
        dirtyBit = 0;

        MoveFirst();
    }   
    return 1; //success
}

void DBFile::Load (Schema &f_schema, const char *loadpath) 
{
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

    //fclose(bulkFile);

    // if (dirtyBit == 1)
    // {
    //     diskFile->AddPage(writePage, writePageIndex);
    // }
}

int DBFile::Open (const char *f_path) 
{
    // diskFile->Open(1, (char *)f_path);
    // writePageIndex = 0;
    // dirtyBit = false;
    // MoveFirst();
    // return 1; // success

    //Just for testing
    diskFile->Open(1, const_cast<char *>(f_path));
    writePageIndex = 0;
    //Reading mode on default
    dirtyBit = 0;
    MoveFirst();
    return 1;
}

void DBFile::MoveFirst () 
{
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

    if (dirtyBit == 1) {
        diskFile->AddPage(writePage, writePageIndex);
        dirtyBit = 0;
    }
    writePageIndex = 0;
    writePage->EmptyItOut();
    //If DBfile is not empty
    if (diskFile->GetLength() > 0) {
        diskFile->GetPage(writePage, writePageIndex);
    }
    //Delete After testing
    cout << "length of file is " << diskFile->GetLength() << "\n";
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

    if (dirtyBit == 1)
        diskFile->AddPage(writePage, writePageIndex);
    diskFile->Close();
    cout << "Closing file, length of file is " << diskFile->GetLength() << "Pages" << "\n";
    return 1;
}

void DBFile::Add (Record &rec) 
{
    // dirtyBit = true;

    // Record tempWriteRecord;
    // tempWriteRecord.Consume(&rec);

    // int status = writePage->Append(&tempWriteRecord);

    // // handle failure case - i.e. if new record cant fit in current page
    // if (status == 0)
    // {
    //     diskFile->AddPage(writePage, writePageIndex);
    //     writePageIndex++;
    //     writePage->EmptyItOut();
    //     writePage->Append(&tempWriteRecord);
    // }

    if (dirtyBit == 0) {
        writePage->EmptyItOut();
        //If file is not empty
        if (diskFile->GetLength() > 0) {
            diskFile->GetPage(writePage, diskFile->GetLength() - 2);
            writePageIndex = diskFile->GetLength() - 2;
        }
        dirtyBit = 1;
    }
    //If reach the end of page
    if (writePage->Append(&rec) == 0) {
        diskFile->AddPage(writePage, writePageIndex++);
        writePage->EmptyItOut();
        writePage->Append(&rec);
    }
}

int DBFile::GetNext (Record &fetchme) 
{
    // fetchme.Copy(currentRecord);
    // int status = readPage->GetFirst(currentRecord);

    // if (status == 0)
    // {
    //     readPageIndex++;

    //     if (readPageIndex >= diskFile->GetLength() - 1)
    //     {
    //         return 0;
    //     }

    //     readPage->EmptyItOut();
    //     diskFile->GetPage(readPage, readPageIndex);
    //     readPage->GetFirst(&fetchme);
    // }
    // return 1;

    //If file is writing, then write page into disk based file, redirect page ot first page
    if (dirtyBit == 1) {
        MoveFirst();
    }
    //If reach the end of page
    if (writePage->GetFirst(&fetchme) == 0) {
        writePageIndex++;
        //If reach the end of file
        if (writePageIndex >= diskFile->GetLength() - 1) {
            return 0;
        }
        //Else get next page
        writePage->EmptyItOut();
        diskFile->GetPage(writePage, writePageIndex);
        writePage->GetFirst(&fetchme);
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
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
