#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

};

class Run {

public:
	Run(File* file, int startPage, int runLength);
	int UpdateTopRecord();
	Record *topRecord; 

private: 
	File* fileBase;
	Page bufferPage;
	int startPage;
	int runLength;
	int curPage;
};

class RecordsChecker {

public:
	bool operator () (Record* left, Record* right);
	RecordsChecker(OrderMaker *order);

private:
	OrderMaker *order;

};

class RunChecker {

public:
	bool operator () (Run* left, Run* right);
	RunChecker(OrderMaker *order);

private:
	OrderMaker *order;

};

typedef struct {
	
	Pipe *pipeIn;
	Pipe *pipeOut;
	OrderMaker *order;
	int runLength;
	
} WorkerArgs;

void* workerFunction(void* arg);

void* recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordsChecker>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunChecker>& runQueue, File& file, Page& bufferPage, int& pageIndex);

#endif
