#ifndef A2_2TEST_DBFILESORTED_H
#define A2_2TEST_DBFILESORTED_H
#include "DBFile.h"
#include <queue>
#include "BigQ.h"


class DBFileSorted :public DBFileCommon
{
    friend class DBFile;

    private:
        File diskFile;
        Page bufferPage;
        off_t pageIndex;
        int isWriting;
        char* out_path = nullptr;
        int calculatedBound = 0;
        int lowerBound;
        int higherBound;

        Pipe* in = new Pipe(100);
        Pipe* out = new Pipe(100);
        pthread_t* thread = nullptr;

        OrderMaker* orderMaker = nullptr;
        int runLength;

        void writingMode();
        void readingMode();
        static void *consumer (void *arg);
        int Run (Record *left, Record *literal, Comparison *c);
        BigQ * bigQ;

    public:
        DBFileSorted ();

        int Create (char *fpath, fType f_type, void *startup);
        int Open (char *fpath);
        int Close ();
        void Load (Schema &myschema, char *loadpath);
        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

#endif //A2_2TEST_DBFILESORTED_H
