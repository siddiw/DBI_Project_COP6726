#include "gtest/gtest.h"
#include <iostream>
#include "Statistics.h"
#include <stdlib.h>
#include "Defs.h"
#include "ParseTree.h"
#include <math.h>
using namespace std;

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;

TEST(AddRelationTest, AddRelTest){
	Statistics s;
	s.AddRel("nation", 25);

	ASSERT_NE(s.relationMap.find("nation"), s.relationMap.end());
	ASSERT_EQ(s.relationMap["nation"]->numOfTuple, 25);
}

TEST(AddAttributeTest, AddAttTest){
	Statistics s;
	s.AddRel("nation", 25);
	s.AddAtt("nation", "n_nationkey", -1);

	ASSERT_NE(s.relationMap["nation"]->attributeMap.find("n_nationkey"), s.relationMap["nation"]->attributeMap.end());
	ASSERT_EQ(s.relationMap["nation"]->attributeMap["n_nationkey"], 25);
}

TEST(GetRelationTest, AddRelAddAttTest){
	Statistics s;
	s.AddRel("nation", 25);
	s.AddAtt("nation", "n_nationkey", -1);

	s.AddRel("region", 10);
	s.AddAtt("region", "r_regionkey", -1);

	ASSERT_EQ(s.getJoinedRelations("n_nationkey"), "nation");
	ASSERT_EQ(s.getJoinedRelations("r_regionkey"), "region");
}

TEST(Query4Test, EstimationTest){
	Statistics s;
    char *relName[] = { "part", "partsupp", "supplier", "nation", "region", "p", "ps", "s", "n", "r"};

	s.AddRel(relName[0], 200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1], 800000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	s.AddAtt(relName[1], "ps_partkey", 200000);
	
	s.AddRel(relName[2], 10000);
	s.AddAtt(relName[2], "s_suppkey",10000);
	s.AddAtt(relName[2], "s_nationkey",25);
	
	s.AddRel(relName[3], 25);
	s.AddAtt(relName[3], "n_nationkey",25);
	s.AddAtt(relName[3], "n_regionkey",5);

	s.AddRel(relName[4],5);
	s.AddAtt(relName[4], "r_regionkey",5);
	s.AddAtt(relName[4], "r_name",5);

	s.CopyRel("part","p");
	s.CopyRel("partsupp","ps");
	s.CopyRel("supplier","s");
	s.CopyRel("nation","n");
	s.CopyRel("region","r");
	char *cnf = "(p_partkey=ps_partkey) AND (p_size = 2)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);

	cnf ="(s_suppkey = ps_suppkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 3);

	cnf =" (s_nationkey = n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 4);

	cnf ="(n_regionkey = r_regionkey) AND (r_name = 'AMERICA') ";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 5);
	//cout<<result<<endl;
	ASSERT_LT(fabs(result-3200), 0.1);
}

int main(int argc, char **argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}