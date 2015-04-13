#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

struct CompareRecords;

class BigQ {

	/*int runLength;
	Pipe *inpipe,*outpipe;
	pthread_t thread;
	OrderMaker *order;*/
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	//void sort_stuff();
	~BigQ ();
	bool comparing(Record *i, Record *j);
//public:
	
	//CompareRecords *cmpr;
};



typedef struct {
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder; 
	int runlen;
	CompareRecords *cmpar;
}thread_args;

typedef struct {
	int run_id;
	int start_page;
	int end_page;
	int cur_page;
}run_data;

struct CompareRecords{
	//OrderMaker *sortorder = new OrderMaker();
	OrderMaker sortorder;
	CompareRecords(OrderMaker *sortorder_in){
		sortorder = *sortorder_in;
	}
	/*bool operator()(Record* const& r1, Record* const& r2)
	//bool operator()(Record* r1, Record* r2)    
	{
        Record* r11 = const_cast<Record*>(r1);
        Record* r22 = const_cast<Record*>(r2);
        ComparisonEngine ce;
        //if (ce.Compare(r1, r2, sortorder) < 0)
	if (ce.Compare(r1, r2, sortorder) < 0)
            return true;
        else
            return false;
    }      */  
	bool operator()(const Record* r1, const Record* r2)
	//bool operator()(Record* r1, Record* r2)    
	{
        //Record* r11 = const_cast<Record*>(r1);
        //Record* r22 = const_cast<Record*>(r2);
        ComparisonEngine ce;
        //if (ce.Compare(r1, r2, sortorder) < 0)
	//cout << "sort order in compare records:	" <<&sortorder << endl;
	if (ce.Compare((Record *)r1, (Record *)r2, &sortorder) < 0)
            return true;
        else
            return false;
    }
};

#endif
