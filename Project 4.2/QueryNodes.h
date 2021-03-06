#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#include <iostream>
#include "Schema.h"
#include "DBFile.h"
#include "DBFileHeap.h"
#include "DBFileSorted.h"
#include "RelOp.h"
#include "Comparison.h"
#include <unordered_map>

using namespace std;

const int BUFFSIZE = 128;

class QueryNodes
{
    public:
        AndList *andList;
        Schema *outputSchema = NULL, *leftSchema = NULL, *rightSchema = NULL;
        NameList *groupingAtts, *attsToSelect;
        FuncOperator *functionOperator;
        QueryNodes *parentNode = NULL, *leftNode = NULL, *rightNode = NULL;
        int leftId = 0, rightId = 0, outputId = 0;
        RelationalOp *relationalOp;
        Pipe *pipe = new Pipe(10000000);

        QueryNodes() {}

        ~QueryNodes()
        {
            delete pipe;
        }

        virtual void printNode()
        {
        }

        virtual void Apply() 
        {
        }

        void makeChild(QueryNodes *child, bool isLeft)
        {
            child->parentNode = this;
            if (isLeft)
            {
                this->leftNode = child;
                this->leftId = child->outputId;
            }
            else
            {
                this->rightNode = child;
                this->rightId = child->outputId;
            }
        }

        void printInOrder()
        {
            if (leftNode != NULL)
                leftNode->printInOrder();
            printNode();
            if (rightNode != NULL)
                rightNode->printInOrder();
        }

        void pushInOrder(vector<int> *res)
        {
            if (leftNode != NULL)
                leftNode->pushInOrder(res);
            res->push_back(outputId);
            if (rightNode != NULL)
                rightNode->pushInOrder(res);
        }
};

class JoinNode : public QueryNodes
{
    public:
        JoinNode(int outputId, Schema *schema, AndList *andList);
        void printNode();
        CNF generateJoinCNF();
        void Apply();
};

class ProjectNode : public QueryNodes
{
    public:
        vector<int> keepMe;
        ProjectNode(int outputId, Schema *schema, vector<int> keepMe);
        void printNode();
        string getKeepMe();
        void Apply();
};

class SelectFileNode : public QueryNodes
{
    public:
        DBFile file;
        bool needToPrintCNF = false;
        SelectFileNode(int outputId, Schema *schema);
        void printNode();
        CNF generateCNF();
        void Apply();
};

class SelectPipeNode : public QueryNodes
{
    public:
        SelectPipeNode(int outputId, Schema *schema);
        void printNode();
        CNF generateCNF();
        void Apply();
};

class DuplicateRemovalNode : public QueryNodes
{
    public:
        DuplicateRemovalNode(int outputId, Schema *schema);
        void printNode();
        void Apply();
};

class SumNode : public QueryNodes
{
    public:
        SumNode(int outputId, Schema *schema, FuncOperator *func);
        void printNode();
        Function generateFunction();
        Function function;
        void Apply();
};

class GroupByNode : public QueryNodes
{
    public:
        GroupByNode(int outputId, Schema *schema, NameList *groupingAtts, FuncOperator *func);
        void printNode();
        OrderMaker generateOrderMaker();
        Function generateFunction();
        Function *function;
        OrderMaker *orderMaker;
        void Apply();
};

class WriteOutNode : public QueryNodes
{
    public:
        char *filePath;
        WriteOutNode(int outputId, Schema *schema);
        void printNode();
        void Apply();
};

#endif