#ifndef DBFILE_H
#define DBFILE_H

#include "DBFileCommon.h"

typedef struct {OrderMaker *myOrder; int runLength;} SortedInfo;

class DBFile
{
    private:
        DBFileCommon* dbFileInstance;

    public:
        DBFile (); 

        int Create (char *fpath, fType file_type, void *startup);
        int Open (char *fpath);
        int Close ();
        void Load (Schema &myschema, char *loadpath);
        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

#endif //DBFILE_H