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
    cout<<"\nLength of closed file: "<<diskFile->Close();
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
