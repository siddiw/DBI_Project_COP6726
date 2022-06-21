
CC = g++ -std=c++11 -Wno-deprecated

tag = -i
gtest_tag = -std=c++11 -lgtest -lgtest_main

ifdef linux
tag = -n
endif

# a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o DBFileCommon.o DBFileHeap.o DBFileSorted.o DBFileTree.o Pipe.o y.tab.o lex.yy.o a2-test.o
# 	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o DBFileCommon.o DBFileHeap.o DBFileSorted.o DBFileTree.o Pipe.o y.tab.o lex.yy.o a2-test.o -lpthread

a4-1.out:   y.tab.o lex.yy.o RelOp.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o Statistics.o a4-test.o
	$(CC) -o a4-1.out y.tab.o lex.yy.o RelOp.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o Statistics.o a4-test.o -lpthread
	
gtest: y.tab.o lex.yy.o RelOp.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o Statistics.o gtest.o
	$(CC) -o gtest.out y.tab.o RelOp.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o Statistics.o gtest.o $(gtest_tag)

gtest.o: gtest.cpp
	$(CC) -g -c gtest.cpp $(gtest_tag)

test.o: test.cc
	$(CC) -g -c test.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc
	
a4-test.o: a4-test.cc
	$(CC) -g -c a4-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

DBFileCommon.o: DBFileCommon.cc
	$(CC) -g -c DBFileCommon.cc

DBFileHeap.o: DBFileHeap.cc
	$(CC) -g -c DBFileHeap.cc

DBFileSorted.o: DBFileSorted.cc
	$(CC) -g -c DBFileSorted.cc

DBFileTree.o: DBFileTree.cc
	$(CC) -g -c DBFileTree.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
