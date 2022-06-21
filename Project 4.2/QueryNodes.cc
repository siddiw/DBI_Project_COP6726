#ifndef QUERY_TREE_NODE_CC
#define QUERY_TREE_NODE_CC

#include "QueryNodes.h"

JoinNode::JoinNode(int outputId, Schema *schema, AndList *andList)
{
    this->outputId = outputId;
    this->outputSchema = schema;
    this->andList = andList;
}

void JoinNode::printNode()
{
    cout << "####################" << endl;
    cout << "Join operation" << endl;
    cout << "Left Input Pipe " << leftId << endl;
    cout << "Right Input Pipe " << rightId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "\nCorresponding CNF: " << endl;
    generateJoinCNF().Print();
    cout << "####################\n"
         << endl;
}

void JoinNode::Apply()
{
    relationalOp = new Join();
    leftNode->Apply();
    rightNode->Apply();
    CNF *cnf = new CNF(this->generateJoinCNF());
    Record literal;
    ((Join *)relationalOp)->Run(*(leftNode->pipe), *(rightNode->pipe), *pipe, *cnf, literal);
    //    leftNode->relationalOp->WaitUntilDone();
    //    rightNode->relationalOp->WaitUntilDone();
    this->relationalOp->WaitUntilDone();
}

CNF JoinNode::generateJoinCNF()
{
    CNF res;
    res.GrowFromParseTree(andList, leftNode->outputSchema, rightNode->outputSchema, *(new Record()));
    return res;
}

SelectFileNode::SelectFileNode(int outputId, Schema *schema)
{
    this->outputId = outputId;
    this->outputSchema = schema;
}

void SelectFileNode::printNode()
{
    cout << "####################" << endl;
    cout << "Select File operation" << endl;
    cout << "Input Pipe " << leftId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "\nCorresponding CNF: " << endl;
    if (needToPrintCNF)
        generateCNF().Print();
    cout << "####################\n"
         << endl;
}

void SelectFileNode::Apply()
{
    relationalOp = new SelectFile();
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(andList, outputSchema, literal);

    ((SelectFile *)relationalOp)->Run(file, *pipe, cnf, literal);
    ((SelectFile *)relationalOp)->WaitUntilDone();
    file.Close();
}

CNF SelectFileNode::generateCNF()
{
    CNF res;
    res.GrowFromParseTree(andList, outputSchema, *(new Record()));
    return res;
}

SelectPipeNode::SelectPipeNode(int outputId, Schema *schema)
{
    this->outputId = outputId;
    this->outputSchema = schema;
}

void SelectPipeNode::printNode()
{
    cout << "####################" << endl;
    cout << "SELECT PIPE Operation" << endl;
    cout << "Input Pipe " << leftId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "\nCorresponding CNF: " << endl;
    generateCNF().Print();
    cout << "####################\n"
         << endl;
}
void SelectPipeNode::Apply()
{
}

CNF SelectPipeNode::generateCNF()
{
    CNF res;
    res.GrowFromParseTree(andList, outputSchema, *(new Record()));
    return res;
}

ProjectNode::ProjectNode(int outputId, Schema *schema, vector<int> keepMe)
{
    this->outputId = outputId;
    this->outputSchema = schema;
    this->keepMe = keepMe;
}

string ProjectNode::getKeepMe()
{
    string res = "(";
    for (int i = 0; i < keepMe.size(); i++)
    {
        res += to_string(keepMe[i]);
        if (i < keepMe.size() - 1)
            res += ", ";
    }
    res += ")";
    return res;
}

void ProjectNode::printNode()
{
    cout << "####################" << endl;
    cout << "Project Operation" << endl;
    cout << "Input Pipe " << leftId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "\nAttributes to keep:" << endl;
    cout << getKeepMe() << endl;
    cout << "####################\n"
         << endl;
}
void ProjectNode::Apply()
{
    this->leftNode->Apply();

    relationalOp = new Project();
    int numIn = this->leftNode->outputSchema->GetNumAtts();
    int numOut = this->outputSchema->GetNumAtts();
    int *attsToKeep = new int[keepMe.size()];

    for (int i = 0; i < keepMe.size(); i++)
    {
        attsToKeep[i] = keepMe[i];
    }
    ((Project *)relationalOp)->Run(*(leftNode->pipe), *pipe, attsToKeep, numIn, numOut);
    this->relationalOp->WaitUntilDone();
}

SumNode::SumNode(int outputId, Schema *schema, FuncOperator *func)
{
    this->outputId = outputId;
    this->outputSchema = schema;
    this->functionOperator = func;
}

void SumNode::printNode()
{
    cout << "####################" << endl;
    cout << "SUM operation" << endl;
    cout << "Left Input Pipe " << leftId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "\nCorresponding Function: " << endl;
    function.Print();
    cout << "####################\n"
         << endl;
}
void SumNode::Apply()
{
    this->leftNode->Apply();
    relationalOp = new Sum();
    ((Sum *)relationalOp)->Run(*(this->leftNode->pipe), *pipe, function);
    this->relationalOp->WaitUntilDone();
}

Function SumNode::generateFunction()
{
    Function res;
    res.GrowFromParseTree(functionOperator, *outputSchema);
    return res;
}

DuplicateRemovalNode::DuplicateRemovalNode(int outputId, Schema *schema)
{
    this->outputId = outputId;
    this->outputSchema = schema;
}

void DuplicateRemovalNode::printNode()
{
    cout << "####################" << endl;
    cout << "Duplicate remove operation" << endl;
    cout << "Left Input Pipe " << leftId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "####################\n"
         << endl;
}
void DuplicateRemovalNode::Apply()
{
    this->leftNode->Apply();
    relationalOp = new DuplicateRemoval();
    ((DuplicateRemoval *)relationalOp)->Run(*(leftNode->pipe), *pipe, *outputSchema);
    this->relationalOp->WaitUntilDone();
}

GroupByNode::GroupByNode(int outputId, Schema *schema, NameList *groupingAtts, FuncOperator *func)
{
    this->outputId = outputId;
    this->outputSchema = schema;
    this->groupingAtts = groupingAtts;
    this->functionOperator = func;
}

void GroupByNode::printNode()
{
    cout << "####################" << endl;
    cout << "GROUPBY operation" << endl;
    cout << "Left Input Pipe " << leftId << endl;
    cout << "Output Pipe " << outputId << endl;
    cout << "\nOutput Schema:" << endl;
    outputSchema->Print();
    cout << "\nCorresponding OrderMaker: " << endl;
    orderMaker->Print();
    cout << "\nCorresponding Function: " << endl;
    function->Print();
    cout << "####################\n"
         << endl;
}
void GroupByNode::Apply()
{
    this->leftNode->Apply();
    relationalOp = new GroupBy();
    ((GroupBy *)relationalOp)->Run(*(leftNode->pipe), *pipe, *orderMaker, *function);
    this->relationalOp->WaitUntilDone();
}

Function GroupByNode::generateFunction()
{
    Function res;
    res.GrowFromParseTree(functionOperator, *outputSchema);
    return res;
}

OrderMaker GroupByNode::generateOrderMaker()
{
    OrderMaker res;
    int num = 0;
    vector<int> groupAttr;
    vector<int> tttype;
    NameList *iterNameList = groupingAtts;

    while (iterNameList != NULL)
    {
        num++;
        groupAttr.push_back(this->outputSchema->Find(iterNameList->name));
        tttype.push_back(this->outputSchema->FindType(iterNameList->name));
        iterNameList = iterNameList->next;
    }

    res.numAtts = num;

    for (int i = 0; i < groupAttr.size(); i++)
    {
        res.whichAtts[i] = groupAttr[i];
        res.whichTypes[i] = (Type)tttype[i];
    }
    return res;
}

WriteOutNode::WriteOutNode(int outputId, Schema *schema)
{
    this->outputId = outputId;
    this->outputSchema = schema;
}

void WriteOutNode::printNode()
{
    cout << "####################" << endl;
    cout << "Write Out Operation" << endl;
    cout << "Left Input Pipe " << leftId << endl;
    cout << "Output File " << filePath << endl;
    cout << "####################\n"
         << endl;
}

void WriteOutNode::Apply()
{
    this->leftNode->Apply();
    relationalOp = new WriteOut();
    FILE *f = fopen(filePath, "w");
    ((WriteOut *)relationalOp)->Run(*(leftNode->pipe), f, *outputSchema);
    this->relationalOp->WaitUntilDone();
    fclose(f);
}

#endif
