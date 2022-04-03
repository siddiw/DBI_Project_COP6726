// #include "TwoWayList.h"
// #include "Record.h"
// #include "Schema.h"
// #include "File.h"
// #include "Comparison.h"
// #include "ComparisonEngine.h"
// #include "DBFile.h"
// #include "Defs.h"
// //Delete after testing
// #include <iostream>

// // stub file .. replace it with your own DBFile.cc

// DBFile::DBFile () {
//     isFileOpen = 0;
//     isWriting = 0;
//     pageIndex = 0;
// }

// int DBFile::Create (const char *f_path, fType f_type, void *startup) {
//     if (isFileOpen == 1) {
//         cerr << "Cannot recreate file since file already opened!" << "\n";
//         return 0;
//     }
//     if(f_type == heap){
//         diskFile.Open(0, const_cast<char *>(f_path));
//         pageIndex = 0;
//         isWriting = 0;
//         isFileOpen = 1;
//         MoveFirst();
//     }
//     return 1;
// }

// void DBFile::Load (Schema &f_schema, const char *loadpath) {
//     if (isFileOpen == 0) {
//         cerr << "Cannot loading while file not open!";
//         return;
//     }
//     FILE *tableFile = fopen (loadpath, "r");
//     Record temp;
//     ComparisonEngine comp;

//     while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
//             this->Add(temp);
//     }
//     if (isWriting == 1)
//         diskFile.AddPage(&bufferPage, pageIndex);
// }

// int DBFile::Open (const char *f_path) {
//     if (isFileOpen == 1) {
//         cerr << "File already opened!" << "\n";
//         return 0;
//     }
//     diskFile.Open(1, const_cast<char *>(f_path));
//     pageIndex = 0;
//     //Reading mode on default
//     isWriting = 0;
//     isFileOpen = 1;
//     MoveFirst();
//     return 1;
// }
// //Stop writing mode and do move first
// void DBFile::MoveFirst () {
//     if (isFileOpen == 0) {
//         cerr << "Cannot MoveFirst while file not opening!" << "\n";
//         return;
//     }
//     if (isWriting == 1) {
//         diskFile.AddPage(&bufferPage, pageIndex);
//         isWriting = 0;
//     }
//     pageIndex = 0;
//     bufferPage.EmptyItOut();
//     //If DBfile is not empty
//     if (diskFile.GetLength() > 0) {
//         diskFile.GetPage(&bufferPage, pageIndex);
//     }
//     //Delete After testing
//     // cout << "length of file is " << diskFile.GetLength() << "\n";
// }
// int DBFile::Close () {
//     if (isFileOpen == 0) {
//         cerr << "File is not opened!" << "\n";
//         return 0;
//     }
//     if (isWriting == 1)
//         diskFile.AddPage(&bufferPage, pageIndex);
//     bufferPage.EmptyItOut();
//     diskFile.Close();
//     isFileOpen = 0;
//     // cout << "Closing file, length of file is " << diskFile.GetLength() << "Pages" << "\n";
//     return 1;
// }
// //Assume file is open
// void DBFile::Add (Record &rec) {
//     if (isFileOpen == 0) {
//         cerr << "Cannot writing while file not opening!" << "\n";
//         return;
//     }
//     //If file is reading, then empty buffer page and redirect to last page of file
//     if (isWriting == 0) {
//         bufferPage.EmptyItOut();
//         //If file is not empty
//         if (diskFile.GetLength() > 0) {
//             diskFile.GetPage(&bufferPage, diskFile.GetLength() - 2);
//             pageIndex = diskFile.GetLength() - 2;
//         }
//         isWriting = 1;
//     }
//     //If reach the end of page
//     if (bufferPage.Append(&rec) == 0) {
//         diskFile.AddPage(&bufferPage, pageIndex++);
//         bufferPage.EmptyItOut();
//         bufferPage.Append(&rec);
//     }
// }
// //Assume file is open
// int DBFile::GetNext (Record &fetchme) {
//     if (isFileOpen == 0) {
//         cerr << "Cannot reading while file not opening!" << "\n";
//         return 0;
//     }
//     //If file is writing, then write page into disk based file, redirect page ot first page
//     if (isWriting == 1) {
//         MoveFirst();
//     }
//     //If reach the end of page
//     if (bufferPage.GetFirst(&fetchme) == 0) {
//         pageIndex++;
//         //If reach the end of file
//         if (pageIndex >= diskFile.GetLength() - 1) {
//             return 0;
//         }
//         //Else get next page
//         bufferPage.EmptyItOut();
//         diskFile.GetPage(&bufferPage, pageIndex);
//         bufferPage.GetFirst(&fetchme);
//     }
//     return 1;
// }

// int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
//     ComparisonEngine comp;
//     //If not reach the end of file
//     while (GetNext(fetchme) == 1) {
//         if (comp.Compare(&fetchme, &literal, &cnf) == 1)
//             return 1;
//     }
//     return 0;
// }

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>

// Default Constructor
DBFile::DBFile () 
{
    diskFile = new File();
    currentReadRecord = new Record();
    writePage = new Page();
    readPage = new Page();
    //TODO: logfile
}

// Default Destructor
DBFile::~DBFile () 
{
    delete diskFile;
    delete currentReadRecord;
    delete writePage;
    delete readPage;
}

// Creates the main diskfile
int DBFile::Create (const char *f_path, fType f_type, void *startup) 
{
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

// Loads multiple records into the DB
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

    fclose(bulkFile);
}

// Opens the main file
int DBFile::Open (const char *f_path) 
{
    diskFile->Open(1, (char *)f_path);
    writePageIndex = 0;
    dirtyBit = false;
    isCurrentEnd = false;
    MoveFirst();
    return 1;
}

// Moves currentReadRecord to first record of the file
void DBFile::MoveFirst () 
{
    if (diskFile->GetLength() > 0)
    {
        readPage->EmptyItOut();
        diskFile->GetPage(this->readPage,0);
        readPage->GetFirst(currentReadRecord);
    }
}

// Close the opened "file"
int DBFile::Close () 
{
    if (dirtyBit == true)
    {
        diskFile->AddPage(writePage, writePageIndex);
        writePageIndex++;
    }
    // TODO: Add a logger function - avoid cout

    diskFile->Close();
    //cout<<"\nLength of closed file: "<<diskFile->Close();
    return 1;
}

// Add a single record
void DBFile::Add (Record &rec) 
{
    dirtyBit = true;

    Record tempWriteRecord;

    // Consume the record, will set "rec" to NULL
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

// Get the record pointed bu currentReadRecord
int DBFile::GetNext (Record &fetchme) 
{
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
}

// Get the next matching record to the given CNF
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    ComparisonEngine comp;

    // While there are still records
    while (GetNext(fetchme) == 1) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

// TODO: Logger function
// void DBFile::InternalLogger (logLevel level, char[] logMessage)
// {
//     // TODO: Add logfile
//     cout<<logMessage;
// }

