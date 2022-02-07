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
	Record *currentReadRecord;	// Current Read Record
	File *diskFile;				// Main Disk File (dbfile)
	Page* writePage;			// Write Buffer Page - used for Add, Load
	Page* readPage;				// Read Buffer Page - used for GetNext, MoveFirst
	bool dirtyBit;				// True when Write Buffer Page has unsaved changes
	off_t writePageIndex;		// Write Page Offset
	off_t readPageIndex;		// Read Page Offset
	bool isCurrentEnd;			// True when currentReadRecord is pointing to last record of the file
	

public:
	DBFile (); 
	~DBFile (); 

	//TODO: void InternalLogger(logLevel level, char[] logMessage);

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
