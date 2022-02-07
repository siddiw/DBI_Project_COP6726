#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;
typedef enum {ERROR, DEBUG, INFO} logLevel;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
	Record *currentReadRecord;
	File *diskFile;
	//File diskFile;
	Page* writePage;
	//Page writePage;
	Page* readPage;
	bool dirtyBit;
	off_t writePageIndex;
	off_t readPageIndex;
	bool isCurrentEnd;
	

public:
	DBFile (); 
	~DBFile (); 

	//void InternalLogger(logLevel level, char[] logMessage);

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
