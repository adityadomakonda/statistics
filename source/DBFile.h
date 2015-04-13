#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;
typedef enum {reading, writing} opType;
typedef enum {pipe_r, file_r,none} last_written;

// stub DBFile header..replace it with your own DBFile.h 

typedef struct {
	OrderMaker *o; 
	int l;
} sort_input;

typedef struct {
  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *sortOrder;
  int runLen;
}thread_arg_123;

class DBFile {
//private:
//public:
	File file; 
	Page *cur_page;
	Page *buffer_page;
	bool modified;
	off_t current_page_index;
	fType type;
	OrderMaker *order; 
	int run_length;
	opType operation_state;
	Pipe *input;
	Pipe *output;
	BigQ *sort_helper_bigq;
	char meta_path[100];
	char file_path[100];
	int flag = 0;
	
public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	int WriteBufferToDisk();
	int sort_merge();


};
#endif
