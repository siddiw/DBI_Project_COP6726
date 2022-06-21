#ifndef A2_2TEST_DBFILECOMMON_H
#define A2_2TEST_DBFILECOMMON_H

#include "Record.h"
#include "Schema.h"

typedef enum {heap, sorted, tree} fType;

class DBFileCommon
{
    public:
        virtual int Create (char *fpath, fType f_type,  void *startup) = 0;
        virtual int Open (char *fpath) = 0;
        virtual int Close () = 0;

        virtual void Load (Schema &myschema, char *loadpath) = 0;

        virtual void MoveFirst () = 0;
        virtual void Add (Record &addme) = 0;
        virtual int GetNext (Record &fetchme) = 0;
        virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};

#endif //A2_2TEST_DBFILECOMMON_H
