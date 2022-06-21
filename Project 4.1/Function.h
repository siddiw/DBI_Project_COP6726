#ifndef FUNCTION_H
#define FUNCTION_H
#include "Record.h"
#include "ParseTree.h"

#define MAX_DEPTH 100

enum ArithOp
{
	PushInt,
	PushDouble,
	ToDouble,
	ToDouble2Down,
	IntUnaryMinus,
	IntMinus,
	IntPlus,
	IntDivide,
	IntMultiply,
	DblUnaryMinus,
	DblMinus,
	DblPlus,
	DblDivide,
	DblMultiply
};

struct Arithmatic
{
	ArithOp myOp;
	void *litInput;
	int recInput;
};

class Function
{
	private:
		Arithmatic *opList;
		int numOps;
		int returnsInt;

	public:
		Function();
		void GrowFromParseTree(struct FuncOperator *parseTree, Schema &mySchema);
		Type RecursivelyBuild(struct FuncOperator *parseTree, Schema &mySchema);
		void Print();
		Type Apply(Record &toMe, int &intResult, double &doubleResult);
};
#endif
