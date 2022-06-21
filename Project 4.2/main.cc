#include <iostream>
#include "ParseTree.h"
#include "QueryNodes.h"
#include "Statistics.h"
#include <string>
#include <fstream>
#include <sstream>

using namespace std;

extern "C" 
{
	int yyparse(void);
}

extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern struct TableList *tables;
extern struct FuncOperator *finalFunction;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
struct AndList* booleanCopy;
extern int queryType;
extern char *outputMode;
extern char *tableName;
extern char *fileToInsert;
extern struct AttrList *attsToCreate;

void pushToVector(vector<AndList>* v) 
{
	AndList new_and_list = *boolean;
	new_and_list.rightAnd = 0;
	v->push_back(new_and_list);
}

string transOperAndToString(Operand* oprnd) 
{
	string ans;
	string s (oprnd->value);
    stringstream stream;

	for (int i = 0; i < s.size(); i++) 
    {
		if (s[i] == '_') 
        {
			return ans;
		}
		else if (s[i] == '.') 
        {
			ans = stream.str();
			return ans;
		}
		else 
        {
			stream << s[i];
		}
	}
}

void getFinalPlan(vector<AndList>* forJoin, Statistics* statistics) 
{
	if (forJoin->size() <= 1)
		return;

	vector<AndList> finalPlan;

	while (forJoin->size() > 1) 
    {
		char* firstRelations[] = {&transOperAndToString((*forJoin)[0].left->left->left)[0], &transOperAndToString((*forJoin)[0].left->left->right)[0]};
		double smallest = statistics->Estimate(&(*forJoin)[0], firstRelations, 2);
		int smallestIndex = 0;

		for (int i = 0; i < forJoin->size(); i++) 
        {
			char *relationsArr[] = {&transOperAndToString((*forJoin)[i].left->left->left)[0], &transOperAndToString((*forJoin)[i].left->left->right)[0]};
			double curEstimate = statistics->Estimate(&(*forJoin)[i], relationsArr, 2);
			if (smallest > curEstimate) 
            {
				smallest = curEstimate;
				smallestIndex = i;
			}
		}
		finalPlan.push_back((*forJoin)[smallestIndex]);
		forJoin->erase(forJoin->begin() + smallestIndex);
	}
	finalPlan.push_back((*forJoin)[0]);
	*forJoin = finalPlan;
}

QueryNodes* retrieveAncient(QueryNodes* curNode) 
{
	if (curNode->parentNode != NULL)
    {
		return retrieveAncient(curNode->parentNode);
    }
	else
    {
		return curNode;
    }
}

void PrintParseTree (struct AndList *andPointer) 
{
    cout << "(";

    while (andPointer) 
    {
        struct OrList *orPtr = andPointer->left;

        while (orPtr) 
        {
            struct ComparisonOp *comPointer = orPtr->left;

            if (comPointer!=NULL) 
            {
                struct Operand *pOperand = comPointer->left;

                if(pOperand!=NULL) 
                {
                    cout<<pOperand->value<<"";
                }

                switch(comPointer->code) 
                {
                    case LESS_THAN:
                        cout<<" < "; 
                        break;

                    case GREATER_THAN:
                        cout<<" > "; 
                        break;

                    case EQUALS:
                        cout<<" = "; 
                        break;

                    default:
                        cout << " unknown code " << comPointer->code;
                }

                pOperand = comPointer->right;

                if(pOperand!=NULL) 
                {
                    cout<<pOperand->value<<"";
                }
            }

            if(orPtr->rightOr) 
            {
                cout<<" OR ";
            }

            orPtr = orPtr->rightOr;
        }

        if(andPointer->rightAnd) 
        {
            cout<<") AND (";
        }

        andPointer = andPointer->rightAnd;
    }
    cout << ")" << endl;
}


int main () 
{
    outputMode = "NONE";

    while(1) 
    {
        Statistics statistics;
        statistics.Read("OriginTables.txt");
        int pipeID = 1;
        cout<<endl;
        cout << "Enter your SQL: ";
        cout<<endl;

        vector <AndList> forJoin, forSelect;
        yyparse();
        cin.clear();
        clearerr(stdin);
        cout << "\nYou Sql has been parsed" << endl;

        if (queryType == 2)
        {
            char filePath[100];
            char tpchPath[100];

            sprintf (filePath, "bin/%s.bin", tableName);
            sprintf (tpchPath, "%s.tbl", tableName);

            ofstream ofs("catalog", ifstream :: app);

            ofs<< endl;
            ofs << "BEGIN" << endl;
            ofs << tableName << endl;
            ofs << tpchPath <<endl;

            while (attsToCreate) 
            {
                ofs << attsToCreate->name << " ";
                switch (attsToCreate->type) 
                {
                    case 0:
                        ofs << "Int" << endl;
                        break;
                    case 1:
                        ofs << "Double" << endl;
                        break;
                    case 2:
                        ofs << "String" << endl;
                        break;
                    default:
                        cout<< "error" << endl;
                        break;
                }
                attsToCreate = attsToCreate->next;
            }
            ofs << "END" << endl;
            ofs.close();
            DBFile file;
            file.Create (filePath, heap, NULL);
            file.Close();
        }
        else if (queryType == 3)
        {
            // DDROP
            char filePath[100];
            char metaPath[100];

            sprintf (filePath, "bin/%s.bin", tableName);
            sprintf (metaPath, "%s.meta", filePath);

            remove (filePath);
            remove (metaPath);

            ostringstream text;
            ifstream in_file("catalog");

            text << in_file.rdbuf();
            string str = text.str();
            string str_search = string(tableName);
            string str_replace = "\n";
            size_t pos1 = str.find(string(tableName));
            size_t pos2 = str.find("END", pos1);
            str.replace(pos1-8, pos2+4, str_replace);
            in_file.close();

            ofstream out_file("catalog");
            out_file << str;
            out_file.close();
        }
        else if (queryType == 4)
        {
            char filePath[100];
            char tpchPath[100];

            sprintf (filePath, "bin/%s.bin", tableName);
            sprintf (tpchPath, "tpch/%s.tbl", tableName);
            DBFile file;
            Schema schema("catalog", tableName);
            file.Open(filePath);
            file.Load(schema, tpchPath);
            file.Close();
        }
        else if (queryType == 5)
        {
            // SET
        }
        else if (queryType == 1) 
        {
            OrList *cur;
            while (boolean != NULL) 
            {
                cur = boolean->left;
                bool selfAndChildValid = (cur != NULL && cur->left->code == EQUALS);
                bool grandChildValid = (cur->left->left->code == NAME && cur->left->right->code == NAME);
                if (selfAndChildValid && grandChildValid) 
                {
                    pushToVector(&forJoin);
                }
                else 
                {
                    pushToVector(&forSelect);
                }
                boolean = boolean->rightAnd;
            }

            unordered_map < string, QueryNodes * > nodeMap;
            QueryNodes *curNode = NULL;
            QueryNodes *orginNode = NULL;

            TableList *iterTables = tables;
            
            while (iterTables != NULL) 
            {
                Schema *schemaForTable = new Schema("catalog", iterTables->tableName);
                nodeMap[iterTables->tableName] = new SelectFileNode(pipeID++, schemaForTable);
                if (iterTables->aliasAs != NULL) 
                {
                    Schema *schemaForalias = new Schema("catalog", iterTables->tableName);
                    statistics.CopyRel(iterTables->tableName, iterTables->aliasAs);
                    schemaForalias->transToAlias(string(iterTables->aliasAs));
                    nodeMap[iterTables->aliasAs] = new SelectFileNode(pipeID++, schemaForalias);
                    char filepath[100];
                    sprintf(filepath, "bin/%s.bin", iterTables->tableName);
                    ((SelectFileNode *) nodeMap[iterTables->aliasAs])->file.Open(filepath);
                }
                iterTables = iterTables->next;
            }

            for (int i = 0; i < forSelect.size(); i++) 
            {
                string relationName =
                        forSelect[i].left->left->left->code == NAME ? transOperAndToString(
                                forSelect[i].left->left->left)
                                                                    : transOperAndToString(
                                forSelect[i].left->left->right);
                SelectFileNode *tempNode = (SelectFileNode *) nodeMap[relationName];
                tempNode->andList = &(forSelect[i]);
                tempNode->needToPrintCNF = true;
                orginNode = tempNode;
            }

            getFinalPlan(&forJoin, &statistics);
            cout << "Optimal Plan has been built" << endl;

            for (int i = 0; i < forJoin.size(); i++) 
            {
                string firstRelation = transOperAndToString(forJoin[i].left->left->left), relTwo = transOperAndToString(
                        forJoin[i].left->left->right);
                string relationName = firstRelation;
                QueryNodes *leftChild = nodeMap[firstRelation];
                QueryNodes *rightChild = nodeMap[relTwo];
                leftChild = retrieveAncient(leftChild);
                rightChild = retrieveAncient(rightChild);

                curNode = new JoinNode(pipeID++, new Schema(leftChild->outputSchema, rightChild->outputSchema),
                                       &forJoin[i]);
                curNode->makeChild(leftChild, true);
                curNode->makeChild(rightChild, false);
                orginNode = curNode;
            }

            if (finalFunction != NULL && distinctFunc == 1) 
            {
                curNode = new DuplicateRemovalNode(pipeID++, orginNode->outputSchema);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            if (finalFunction != NULL && groupingAtts == NULL) 
            {
                curNode = new SumNode(pipeID++, NULL, finalFunction);
                ((SumNode *) curNode)->function.GrowFromParseTree(finalFunction, *orginNode->outputSchema);
                Attribute *DA;
                if (((SumNode *) curNode)->function.returnInt()) 
                {
                    DA = new Attribute{"SUM", Int};
                } 
                else 
                {
                    DA = new Attribute{"SUM", Double};
                }
                ((SumNode *) curNode)->outputSchema = new Schema("out_sch", 1, DA);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            if (finalFunction != NULL && groupingAtts != NULL) 
            {
                curNode = new GroupByNode(pipeID++, orginNode->outputSchema, groupingAtts, finalFunction);
                ((GroupByNode *) curNode)->function = new Function(((GroupByNode *) curNode)->generateFunction());
                ((GroupByNode *) curNode)->orderMaker = new OrderMaker(((GroupByNode *) curNode)->generateOrderMaker());
                Attribute *DA;

                if (((GroupByNode *) curNode)->function->returnInt()) 
                {
                    DA = new Attribute{"SUM", Int};
                } 
                else 
                {
                    DA = new Attribute{"SUM", Double};
                }
                ((GroupByNode *) curNode)->outputSchema = new Schema("out_sch", 1, DA);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            if (distinctAtts == 1) 
            {
                curNode = new DuplicateRemovalNode(pipeID++, orginNode->outputSchema);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            if (attsToSelect != NULL) 
            {
                vector<int> attributesKeep;
                NameList *iterAtt = attsToSelect;
                string attribute;

                while (iterAtt != 0) 
                {
                    attributesKeep.push_back(orginNode->outputSchema->Find(const_cast<char *>(iterAtt->name)));
                    iterAtt = iterAtt->next;
                }
                Schema *newSchema = new Schema(orginNode->outputSchema, attributesKeep);
                curNode = new ProjectNode(pipeID++, newSchema, attributesKeep);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            cout << endl;
            cout << "OUTPUT MODE: " << outputMode << endl;
            cout<<"--------------"<< endl;

            if(strcmp(outputMode, "NONE") == 0)
            {
                curNode->printInOrder();
            }
            else
            {
                // Not needed
            }

            for (auto it = nodeMap.begin(); it != nodeMap.end(); it++) 
            {
                delete it->second;
            }
        }
    }
}


