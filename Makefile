CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o bin/a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o -lfl -lpthread
	
test.o: source/test.cc
	$(CC) -g -c source/test.cc

Statistics.o: source/Statistics.cc
	$(CC) -g -c source/Statistics.cc

Comparison.o: source/Comparison.cc
	$(CC) -g -c source/Comparison.cc
	
ComparisonEngine.o: source/ComparisonEngine.cc
	$(CC) -g -c source/ComparisonEngine.cc

Pipe.o: source/Pipe.cc
	$(CC) -g -c source/Pipe.cc

RelOp.o: source/RelOp.cc
	$(CC) -g -c source/RelOp.cc

BigQ.o: source/BigQ.cc
	$(CC) -g -c source/BigQ.cc
	
DBFile.o: source/DBFile.cc
	$(CC) -g -c source/DBFile.cc

Function.o: source/Function.cc
	$(CC) -g -c source/Function.cc

File.o: source/File.cc
	$(CC) -g -c source/File.cc

Record.o: source/Record.cc
	$(CC) -g -c source/Record.cc

Schema.o: source/Schema.cc
	$(CC) -g -c source/Schema.cc
	
y.tab.o: source/Parser.y
	yacc -d source/Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: source/Lexer.l
	lex  source/Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
