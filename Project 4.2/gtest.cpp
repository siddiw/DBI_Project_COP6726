#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include "QueryNodes.h"


using namespace std;


class a42Testing : public ::testing::Test {

protected:
    a42Testing() {
        // You can do set-up work for each test here.
    }

    ~a42Testing() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }
    void SetUp() override {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};



TEST_F(a42Testing, Join_SelectFile_Node_ConstructorTest){
    QueryNodes* node = new JoinNode(1, NULL, NULL);
    QueryNodes* node2 = new SelectFileNode(1, NULL);
}

TEST_F(a42Testing, Join_SelectFile_Node_DestructorTest){
    QueryNodes* node = new JoinNode(1, NULL, NULL);
    QueryNodes* node2 = new SelectFileNode(1, NULL);
    delete node;
    delete node2;
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
