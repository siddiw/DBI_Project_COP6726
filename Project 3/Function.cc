#include "Function.h"
#include <iostream>
#include <stdlib.h>

Function ::Function()
{
	opList = new Arithmatic[MAX_DEPTH];
}

Type Function ::RecursivelyBuild(struct FuncOperator *parseTree, Schema &mySchema)
{
	if (parseTree->right == 0 && parseTree->leftOperand == 0 && parseTree->code == '-')
	{
		Type currType = RecursivelyBuild(parseTree->leftOperator, mySchema);

		switch (currType)
		{
		case Int:
			opList[numOps].myOp = IntUnaryMinus;
			numOps++;
			return Int;

		case Double:
			opList[numOps].myOp = DblUnaryMinus;
			numOps++;
			return Double;

		default:
			cerr << "Weird type.\n";
			exit(1);
		}
	}
	else if (parseTree->leftOperator == 0 && parseTree->right == 0)
	{

		if (parseTree->leftOperand->code == NAME)
		{
			int myNum = mySchema.Find(parseTree->leftOperand->value);
			if (myNum == -1)
			{
				cerr << "Error!  Attribute in arithmatic expression was not found.\n";
				exit(1);
			}

			int currType = mySchema.FindType(parseTree->leftOperand->value);

			switch (currType)
			{
			case Int:
				opList[numOps].myOp = PushInt;
				opList[numOps].recInput = myNum;
				opList[numOps].litInput = 0;
				numOps++;
				return Int;

			case String:
				cerr << "Error!  No arithmatic ops over strings are allowed.\n";
				exit(1);

			default:
				opList[numOps].myOp = PushDouble;
				opList[numOps].recInput = myNum;
				opList[numOps].litInput = 0;
				numOps++;
				return Double;
			}
		}
		else if (parseTree->leftOperand->code == INT)
		{
			opList[numOps].myOp = PushInt;
			opList[numOps].recInput = -1;
			opList[numOps].litInput = (void *)(new int);
			*((int *)opList[numOps].litInput) = atoi(parseTree->leftOperand->value);
			numOps++;
			return Int;
		}
		else
		{
			opList[numOps].myOp = PushDouble;
			opList[numOps].recInput = -1;
			opList[numOps].litInput = (void *)(new double);
			*((double *)opList[numOps].litInput) = atof(parseTree->leftOperand->value);
			numOps++;
			return Double;
		}
	}
	else
	{
		Type currTypeLeft = RecursivelyBuild(parseTree->leftOperator, mySchema);
		Type currTypeRight = RecursivelyBuild(parseTree->right, mySchema);

		if (currTypeLeft == Int && currTypeRight == Int)
		{
			switch (parseTree->code)
			{
				case '+':
					opList[numOps].myOp = IntPlus;
					numOps++;
					return Int;

				case '-':
					opList[numOps].myOp = IntMinus;
					numOps++;
					return Int;

				case '*':
					opList[numOps].myOp = IntMultiply;
					numOps++;
					return Int;

				case '/':
					opList[numOps].myOp = IntDivide;
					numOps++;
					return Int;

				default:
					cerr << "Weird type!!!\n";
					exit(1);
			}
		}

		if (currTypeLeft == Int)
		{
			opList[numOps].myOp = ToDouble2Down;
			numOps++;
		}

		if (currTypeRight == Int)
		{
			opList[numOps].myOp = ToDouble;
			numOps++;
		}

		if (parseTree->code == '+')
		{
			opList[numOps].myOp = DblPlus;
			numOps++;
			return Double;
		}
		else if (parseTree->code == '-')
		{
			opList[numOps].myOp = DblMinus;
			numOps++;
			return Double;
		}
		else if (parseTree->code == '*')
		{
			opList[numOps].myOp = DblMultiply;
			numOps++;
			return Double;
		}
		else if (parseTree->code == '/')
		{
			opList[numOps].myOp = DblDivide;
			numOps++;
			return Double;
		}
		else
		{
			cerr << "Weird type!!!\n";
			exit(1);
		}
	}
}

void Function ::GrowFromParseTree(struct FuncOperator *parseTree, Schema &mySchema)
{
	numOps = 0;
	Type resType = RecursivelyBuild(parseTree, mySchema);

	if (resType == Int)
		returnsInt = 1;
	else
		returnsInt = 0;
}

void Function ::Print()
{
}

Type Function ::Apply(Record &toMe, int &intResult, double &doubleResult)
{
	double stack[MAX_DEPTH];
	double *lastPos = stack - 1;
	char *bits = toMe.bits;

	for (int i = 0; i < numOps; i++)
	{

		switch (opList[i].myOp)
		{
		case PushInt:
			lastPos++;
			if (opList[i].recInput >= 0)
			{
				int pointer = ((int *)toMe.bits)[opList[i].recInput + 1];
				*((int *)lastPos) = *((int *)&(bits[pointer]));
			}
			else
			{
				*((int *)lastPos) = *((int *)opList[i].litInput);
			}

			break;

		case PushDouble:
			lastPos++;
			if (opList[i].recInput >= 0)
			{
				int pointer = ((int *)toMe.bits)[opList[i].recInput + 1];
				*((double *)lastPos) = *((double *)&(bits[pointer]));
			}
			else
			{
				*((double *)lastPos) = *((double *)opList[i].litInput);
			}

			break;

		case ToDouble:

			*((double *)lastPos) = *((int *)lastPos);
			break;

		case ToDouble2Down:

			*((double *)(lastPos - 1)) = *((int *)(lastPos - 1));
			break;

		case IntUnaryMinus:

			*((int *)lastPos) = -(*((int *)lastPos));
			break;

		case DblUnaryMinus:

			*((double *)lastPos) = -(*((double *)lastPos));
			break;

		case IntMinus:

			*((int *)(lastPos - 1)) = *((int *)(lastPos - 1)) -
									  *((int *)lastPos);
			lastPos--;
			break;

		case DblMinus:

			*((double *)(lastPos - 1)) = *((double *)(lastPos - 1)) -
										 *((double *)lastPos);
			lastPos--;
			break;

		case IntPlus:

			*((int *)(lastPos - 1)) = *((int *)(lastPos - 1)) +
									  *((int *)lastPos);
			lastPos--;
			break;

		case DblPlus:

			*((double *)(lastPos - 1)) = *((double *)(lastPos - 1)) +
										 *((double *)lastPos);
			lastPos--;
			break;

		case IntDivide:

			*((int *)(lastPos - 1)) = *((int *)(lastPos - 1)) /
									  *((int *)lastPos);
			lastPos--;
			break;

		case DblDivide:

			*((double *)(lastPos - 1)) = *((double *)(lastPos - 1)) /
										 *((double *)lastPos);
			lastPos--;
			break;

		case IntMultiply:

			*((int *)(lastPos - 1)) = *((int *)(lastPos - 1)) *
									  *((int *)lastPos);
			lastPos--;
			break;

		case DblMultiply:

			*((double *)(lastPos - 1)) = *((double *)(lastPos - 1)) *
										 *((double *)lastPos);
			lastPos--;
			break;

		default:

			cerr << "Had a function operation I did not recognize!\n";
			exit(1);
		}
	}

	if (lastPos != stack)
	{
		cerr << "During function evaluation, we did not have exactly one value ";
		cerr << "left on the stack.  BAD!\n";
		exit(1);
	}

	if (returnsInt)
	{
		intResult = *((int *)lastPos);
		return Int;
	}
	else
	{
		doubleResult = *((double *)lastPos);
		return Double;
	}
}
