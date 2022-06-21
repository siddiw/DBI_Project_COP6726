#include "Statistics.h"
#include <string>
#include <string.h>

Statistics::Statistics(Statistics &copyMe)
{
    for (unordered_map<string, RelationInfo *>::iterator iter = relationMap.begin(); iter != relationMap.end(); iter++)
    {
        RelationInfo *currentRelPtr = iter->second;
        RelationInfo *newRelationPtr = new RelationInfo(currentRelPtr->relationName, currentRelPtr->numOfTuple);

        for (unordered_map<string, int>::iterator kvPair = currentRelPtr->attributeMap.begin(); kvPair != currentRelPtr->attributeMap.end(); kvPair++)
        {
            newRelationPtr->attributeMap[kvPair->first] = kvPair->second;
        }

        for (string jr : currentRelPtr->joinedRel)
        {
            if (jr.compare(currentRelPtr->relationName) != 0)
                newRelationPtr->joinedRel.insert(jr);
        }
        copyMe.relationMap[newRelationPtr->relationName] = newRelationPtr;
    }
}

Statistics::Statistics()
{
}

Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    string sName = string(relName);
    relationMap[sName] = new RelationInfo(sName, numTuples);
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relationName = string(relName);
    string attributeName = string(attName);

    if (relationMap.count(relationName) == 0)
    {
        return;
    }

    if (numDistincts == -1)
    {
        numDistincts = relationMap[relationName]->numOfTuple;
    }

    relationMap[relationName]->attributeMap[attributeName] = numDistincts;
}

void Statistics::CopyRel(char *oldRelName, char *newRelName)
{
    string sOldRelName = string(oldRelName);
    string sNewRelName = string(newRelName);

    if (relationMap.count(sOldRelName) == 0)
        return;

    RelationInfo *currRel = relationMap[sOldRelName];
    RelationInfo *newRel = new RelationInfo(sNewRelName, currRel->numOfTuple);

    for (unordered_map<string, int>::iterator kvPair = currRel->attributeMap.begin(); kvPair != currRel->attributeMap.end(); kvPair++)
    {
        newRel->attributeMap[sNewRelName + "." + kvPair->first] = kvPair->second;
    }

    for (string jr : currRel->joinedRel)
    {
        if (jr.compare(sOldRelName) != 0)
            newRel->joinedRel.insert(jr);
    }

    relationMap[sNewRelName] = newRel;
}

void Statistics::Read(char *fromWhere)
{
    ifstream ipStream;
    RelationInfo *currRel;
    ipStream.open(fromWhere);
    string word;

    while (ipStream >> word)
    {
        if (word.compare("Relation:") == 0)
        {
            ipStream >> word;
            string curRelationName = word;
            ipStream >> word;
            ipStream >> word;
            int numOfTuple = stoi(word);
            currRel = new RelationInfo(curRelationName, numOfTuple);
        }
        else if (word.compare("joinedRel:") == 0)
        {
            ipStream >> word;
            currRel->joinedRel.insert(word);
        }
        else if (word.compare("Attribute:") == 0)
        {
            ipStream >> word;
            string curAttributeName = word;
            ipStream >> word;
            ipStream >> word;
            int numOfDistinct = stoi(word);

            if (numOfDistinct == -1)
                numOfDistinct = currRel->numOfTuple;

            currRel->attributeMap[curAttributeName] = numOfDistinct;
        }
        else if (word.compare("EndOfRelation") == 0)
        {
            relationMap[currRel->relationName] = currRel;
        }
    }

    ipStream.close();
}

void Statistics::Write(char *fromWhere)
{
    ofstream outputStream;
    outputStream.open(fromWhere);
    unordered_map<string, RelationInfo *>::iterator pair;

    for (pair = relationMap.begin(); pair != relationMap.end(); pair++)
    {
        string curRelationName = pair->first;
        RelationInfo *curRelation = pair->second;
        int numOfTuple = curRelation->numOfTuple;
        outputStream << "Relation: " << curRelationName << " numOfTuple: " << numOfTuple << "\n";

        for (string rel : curRelation->joinedRel)
        {
            outputStream << "joinedRel: " << rel << "\n";
        }

        unordered_map<string, int> *curAttributes = &(curRelation->attributeMap);
        unordered_map<string, int>::iterator pair2;

        for (pair2 = curAttributes->begin(); pair2 != curAttributes->end(); pair2++)
        {
            string curAttributeName = pair2->first;
            int numOfDistinct = pair2->second;
            outputStream << "Attribute: " << curAttributeName << " numOfDistinct: " << numOfDistinct << "\n";
        }

        outputStream << "EndOfRelation"<< "\n";
    }
    outputStream.close();
}

double Statistics::Estimate(struct AndList *tree, char **relationNames, int numToJoin)
{
    double res = 1.0;
    unordered_map<string, long> uniqueList;

    if (helpPartitionAndParseTree(tree, relationNames, numToJoin, uniqueList))
    {
        string subsetName = "G";
        unordered_map<string, long> tval;

        int subsetSize = numToJoin;
        int i = -1;

        while (++i < subsetSize)
        {
            subsetName = subsetName + "," + relationNames[i];
        }

        i = -1;

        while (++i < numToJoin)
        {
            string serRes = serializationJoinedRelations(relationMap[relationNames[i]]->joinedRel);
            tval[serRes] = relationMap[relationNames[i]]->numOfTuple;
        }

        res = 1000.0;

        while (tree != nullptr)
        {
            res = helpTuplesEstimate(tree->left, uniqueList) * res;
            tree = tree->rightAnd;
        }

        unordered_map<string, long>::iterator ti = tval.begin();

        while (ti != tval.end())
        {
            res *= ti->second;
            ti++;
        }
    }
    else
    {
        return -1.0;
    }

    res = res / 1000.0;
    return res;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    double r = Estimate(parseTree, relNames, numToJoin);
    long numTuples = (long)round(r);
    string subsetName = "G";
    int i = numToJoin;

    while (i-- > 0)
    {
        subsetName = subsetName + "," + string(relNames[i]);
    }

    i = numToJoin;

    while (--i >= 0)
    {
        relationMap[relNames[i]]->joinedRel = getJoinedRelations(subsetName);
        relationMap[relNames[i]]->numOfTuple = numTuples;
    }
}

set<string> getJoinedRelations(string subsetName)
{
    set<string> joinedRelation;
    subsetName = subsetName + ",";
    int size = subsetName.size();
    int index = 0;

    while (subsetName.size() > 0)
    {
        index = subsetName.find(",");
        string sub = subsetName.substr(0, index);
        
        if (sub.compare("G") != 0)
        {
            joinedRelation.insert(sub);
        }
        subsetName.erase(0, index + 1);
    }
    return joinedRelation;
}

string serializationJoinedRelations(set<string> joinedRelation)
{
    if (joinedRelation.size() == 1)
        return *joinedRelation.begin();

    string result = "G";

    for (string rel : joinedRelation)
        result = result + "," + rel;

    return result;
}

bool Statistics::helpPartitionAndParseTree(struct AndList *tree, char *relationNames[], int sizeOfAttributesJoin, unordered_map<string, long> &uniqueList)
{
    bool res = true;

    while (!(tree == NULL || !res))
    {
        struct OrList *orListTop;
        orListTop = tree->left;
        while (!(orListTop == NULL || !res))
        {
            struct ComparisonOp *cmpOp = orListTop->left;
            if (!(cmpOp->left->code != NAME || cmpOp->code != STRING || helpAttributes(cmpOp->left->value, relationNames, sizeOfAttributesJoin, uniqueList)))
            {
                cout << "\n"
                     << cmpOp->left->value << " Does Not Exist";
                res = false;
            }
            if (!(cmpOp->right->code != NAME || cmpOp->code != STRING || helpAttributes(cmpOp->right->value, relationNames, sizeOfAttributesJoin, uniqueList)))
            {
                res = false;
            }
            orListTop = orListTop->rightOr;
        }
        tree = tree->rightAnd;
    }

    if (false == res)
        return res;

    unordered_map<string, int> tbl;
    int i = 0;

    while (i < sizeOfAttributesJoin)
    {
        string gn = serializationJoinedRelations(relationMap[relationNames[i]]->joinedRel);
        if (tbl.find(gn) == tbl.end())
        {
            tbl[gn] = relationMap[string(relationNames[i])]->joinedRel.size() - 1;
        }
        else
            tbl[gn]--;
        i++;
    }

    unordered_map<string, int>::iterator ti = tbl.begin();
    ;
    while (ti != tbl.end())
    {
        if (ti->second != 0)
        {
            res = false;
            break;
        }
        ti++;
    }
    return res;
}

bool Statistics::helpAttributes(char *v, char *relationNames[], int numberOfJoinAttributes, unordered_map<string, long> &uniqueList)
{
    for (int i = 0; i < numberOfJoinAttributes; i++)
    {
        unordered_map<string, RelationInfo *>::iterator itr;
        itr = relationMap.find(relationNames[i]);

        if (relationMap.end() == itr)
        {
            return false;
        }
        else
        {
            string rel = string(v);

            if (itr->second->attributeMap.end() != itr->second->attributeMap.find(rel))
            {
                uniqueList[rel] = itr->second->attributeMap.find(rel)->second;
                return true;
            }
        }
    }
    return false;
}

double Statistics::helpTuplesEstimate(struct OrList *orList, unordered_map<string, long> &uniqueList)
{
    unordered_map<string, double> currMap;

    while (!(orList == NULL && true))
    {
        struct ComparisonOp *comp = orList->left;
        string key = string(comp->left->value);

        if (currMap.end() == currMap.find(key))
        {
            currMap[key] = 0.0;
        }

        if (!(comp->code != 1 && comp->code != 2))
        {
            currMap[key] = currMap[key] + 1.0 / 3;
        }
        else
        {
            string leftVal = string(comp->left->value);
            long maxVal = uniqueList[leftVal];

            if (4 == comp->right->code)
            {
                string rightKeyVal = string(comp->right->value);
                if (uniqueList[rightKeyVal] > maxVal)
                    maxVal = uniqueList[rightKeyVal];
            }
            currMap[key] = 1.0 / maxVal + currMap[key] + 0;
        }
        orList = orList->rightOr;
    }

    unordered_map<string, double>::iterator tempItr;
    tempItr = currMap.begin();
    double selectivity = 1.0;

    while (tempItr != currMap.end())
    {
        selectivity *= (1.0 - tempItr->second);
        tempItr++;
    }

    selectivity = (1.0 - selectivity);
    return selectivity;
}