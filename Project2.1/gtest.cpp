#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
using namespace std;

class BigQTest : public ::testing::Test {
protected:
    BigQTest() {
        
    }

    ~BigQTest() override {
        
    }
    void SetUp() override {
        
    }

    void TearDown() override {
        
    }
    // File file;
    char* regionFileName = "localTest/region.bin";
    char* lineitemFileName = "localTest/lineitem.bin";
    char* testFileName = "localTest/test.bin";
};

TEST_F(BigQTest, UpdateTopRecordTest) {
    File file;
    file.Open(1, regionFileName);
    class Run* run = new class Run(&file, 0, 1);
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record tempRecord;
    bufferPage.GetFirst(&tempRecord);
    while (bufferPage.GetFirst(&tempRecord) == 1) {
        EXPECT_EQ(run->UpdateTopRecord(), 1);
    }
    EXPECT_EQ(run->UpdateTopRecord(), 0);
    file.Close();
}

TEST_F(BigQTest, RunCheckerTest) {
    File file;
    file.Open(1, lineitemFileName);
    Schema* scheme = new Schema("catalog", "lineitem");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<class Run*, vector<class Run*>, RunChecker> runQueue (order);
    ComparisonEngine comparisonEngine;

    class Run* run1 = new class Run(&file, 0, 1);
    class Run* run2 = new class Run(&file, 1, 1);
    runQueue.push(run1);
    runQueue.push(run2);

    Record firstRecord, secondRecord;
    firstRecord.Copy(runQueue.top()->topRecord);
    runQueue.pop();
    secondRecord.Copy(runQueue.top()->topRecord);
    runQueue.pop();
    EXPECT_EQ(comparisonEngine.Compare(&firstRecord, &secondRecord, order), -1);

    file.Close();
}

TEST_F(BigQTest, RecordCheckerTest) {
    File file;
    file.Open(1, regionFileName);
    Schema* scheme = new Schema("catalog", "region");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<Record*, vector<Record*>, RecordsChecker> recordQueue (order);
    ComparisonEngine comparisonEngine;

    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record* readRecord = new Record;
    while (bufferPage.GetFirst(readRecord)) {
        recordQueue.push(readRecord);
        readRecord = new Record;
    }

    bufferPage.EmptyItOut();
    file.GetPage(&bufferPage, 0);
    Record rec[2];
    Record *last = NULL, *prev = NULL;
    int i = 0;
    while (bufferPage.GetFirst(&rec[i%2]) == 1) {
        prev = last;
        last = &rec[i%2];
        if (prev && last) {
            EXPECT_EQ(comparisonEngine.Compare(prev, last, order), -1);
        }
        i++;
    }
    file.Close();
}


TEST_F(BigQTest, RecordQueueToRunTest) {
    File file;
    file.Open(1, regionFileName);
    Schema* scheme = new Schema("catalog", "region");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<Record*, vector<Record*>, RecordsChecker> recordQueue (order);
    ComparisonEngine comparisonEngine;
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record* readindRecord = new Record;
    while (bufferPage.GetFirst(readindRecord)) {
        recordQueue.push(readindRecord);
        readindRecord = new Record;
    }

    File testFile;
    testFile.Open(0, testFileName);
    Page testPage;
    int pageIndex = 0;
    priority_queue<class Run*, vector<class Run*>, RunChecker> runQueue (order);
    recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
    EXPECT_EQ(recordQueue.size(), 0);
    EXPECT_EQ(runQueue.size(), 1);
    file.Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
