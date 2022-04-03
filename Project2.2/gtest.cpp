#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "DBFileHeap.cc"
#include "DBFileSorted.cc"
#include "DBFileSorted.h"
#include "DBFileTree.h"
#include "DBFileTree.cc"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class DBFileSortedTest : public ::testing::Test
{
    protected:
        DBFileSortedTest() {
        }

        ~DBFileSortedTest() override {
        }

        void SetUp() override {
            dbfile = new DBFile();
            orderMaker.numAtts = 1;
            orderMaker.whichAtts[0] = 4;
            orderMaker.whichTypes[0] = String;
        }

        void TearDown() override {
            delete dbfile;
        }

        DBFile* dbfile;
        OrderMaker orderMaker;
};

TEST_F(DBFileSortedTest, TC_Create) 
{
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    int aa = dbfile->Create("./gtest.bin", sorted, &startup);
    cout<<"=="<<aa<<endl;
    dbfile->Close();
}

TEST_F(DBFileSortedTest, TC_Open_Negative) 
{
    DBFile dbFile;
    EXPECT_EQ(dbFile.Open(nullptr), 0);
}

TEST_F(DBFileSortedTest, TC_Open_Positive) 
{
    EXPECT_EQ(dbfile->Open("./gtest.bin"), 1);
    dbfile->Close();
}

TEST_F(DBFileSortedTest, TC_Close) 
{
    dbfile->Open("./gtest.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileSortedTest, TC_OpenAndClose) 
{
    dbfile->Open("./gtest.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileSortedTest, TC_CreateAndClose) 
{
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    dbfile->Create("./gtest.bin", sorted, &startup);
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileSortedTest, TC_GetNext) 
{
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    dbfile->Open("./gtest.bin");
    Record rec;
    EXPECT_EQ(dbfile->GetNext(rec), 0);
    dbfile->Close();
}

int main(int argc, char **argv) 
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
