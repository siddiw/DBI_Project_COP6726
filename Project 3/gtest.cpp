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
#include "RelOp.cc"
#include "RelOp.h"
#include "Function.h"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class RefOpTest : public ::testing::Test {

protected:
    RefOpTest() {
    }

    ~RefOpTest() override {
    }
    void SetUp() override {
    }
    void TearDown() override {
    }
};

TEST_F(RefOpTest, TestConstructor) {
    SelectFile sf;
}

TEST_F(RefOpTest, TestPipeWaitUntilDone) {
    SelectPipe sp;
    sp.WaitUntilDone();
}

TEST_F(RefOpTest, TestFileWaitUntilDone) {
    SelectFile sf;
    sf.WaitUntilDone();
}

TEST_F(RefOpTest, TestUse_n_Pages) {
    GroupBy* sf = new GroupBy();
    int VALUE = 100;
    sf->Use_n_Pages(VALUE);
    EXPECT_EQ(VALUE, sf->use_n_pages);
}

TEST_F(RefOpTest, TestSum)
{
    Sum *s = new Sum();
    s->WaitUntilDone();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
