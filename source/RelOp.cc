#include <string>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "RelOp.h"
#include "Function.h"

///////////////////////////////////////////////////select file///////////////////////////////////////////////////

void * SelectFile::Run_SelectFile(void *arg){
	SelectFile *run_sf;
	cout<< "Run_SelectFile started"<<endl;
	int count = 0;	
	run_sf = (SelectFile *)arg;
	Record temp;
	run_sf->in_File->MoveFirst();
	while(run_sf->in_File->GetNext(temp, *run_sf->cnf, *run_sf->literal_rec)){
		run_sf->out_Pipe->Insert(&temp);
		count ++;
	}
	cout << "records added into pipe in SelectFile: " << count << endl;
	run_sf->out_Pipe->ShutDown();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	in_File = &inFile;
	out_Pipe = &outPipe;
	cnf = &selOp;
	literal_rec = &literal;
	pthread_create (&thread, NULL, SelectFile::Run_SelectFile, this);
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
	cout<< "Run_SelectFile thread Joined"<<endl;
}

void SelectFile::Use_n_Pages (int runlen) {

}


////////////////////////////////////////////select pipe///////////////////////////////////////////////////

void * SelectPipe::Run_SelectPipe(void *arg){
	cout<< "Run_SelectPipe started"<<endl;	
	SelectPipe *run_sp;
	run_sp = (SelectPipe *)arg;
	int count = 0;
	Record temp;
	ComparisonEngine comp;
	while(run_sp->in_Pipe->Remove(&temp)){
		if(comp.Compare(&temp,run_sp->literal_rec,run_sp->cnf)){
			run_sp->out_Pipe->Insert(&temp);
			count ++;	
			
		}
	}
	run_sp->out_Pipe->ShutDown();
	cout << "number of records inserted into the pipe in SelectPipe: " << count << endl;
	
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	in_Pipe = &inPipe;
	out_Pipe = &outPipe;
	cnf = &selOp;
	literal_rec = &literal;
	pthread_create (&thread, NULL, SelectPipe::Run_SelectPipe, this);

} 

void SelectPipe::WaitUntilDone () {
	
	pthread_join (thread, NULL);
	cout<< "Run_SelectPipe thread joined"<<endl;	
	
}

void SelectPipe::Use_n_Pages (int n) {

}

//////////////////////////////////////////////Project////////////////////////////////////////////////////////

void* Project::Run_Project(void* arg){
	cout<< "Run_Project started"<<endl;	
	int count = 0;
	Project *run_p;
	run_p = (Project *)arg;
	Record temp;
	while(run_p->in_Pipe->Remove(&temp)){
		temp.Project(run_p->Order_of_Atts,run_p->OutputnumAtts, run_p->InputnumAtts);
		run_p->out_Pipe->Insert(&temp);
		//cout<<"p";
		count ++;
	}
	cout << "number of records inserted into the pipe in Project: " << count << endl;
	run_p->out_Pipe->ShutDown();	
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	in_Pipe = &inPipe;
	out_Pipe = &outPipe;
	Order_of_Atts = keepMe;
	InputnumAtts = numAttsInput;
	OutputnumAtts = numAttsOutput;
	pthread_create (&thread, NULL, Project::Run_Project, this);
}

void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
	cout<< "Run_Project thread joined"<<endl;	
}
void Project::Use_n_Pages (int n) {

}

//////////////////////////////////////////////////Join///////////////////////////////////////////////////////

void* Join::Run_Join(void* arg)
{
	cout << "Run_Join started" << endl;
	Join *run_j = (Join *)arg;
	ComparisonEngine comp;
	int count = 0;
	Record *leftRecord, *rightRecord,*nextleftrec , *nextrightrec;
	Record *final_Record;

	OrderMaker *LeftOrder = new OrderMaker();
	OrderMaker *RightOrder = new OrderMaker();
	
	Pipe Pipe_Left(100);
	Pipe Pipe_Right(100);
	
	
	run_j->cnf->GetSortOrders(*LeftOrder, *RightOrder);
	cout<< "sorting records in both the pipes" << endl;
	BigQ bigq1(*(run_j->inPipe_L), Pipe_Left, *LeftOrder, run_j->numOfPages);
	BigQ bigq2(*(run_j->inPipe_R), Pipe_Right, *RightOrder, run_j->numOfPages);
	
	leftRecord = new Record();
	rightRecord = new Record();
	nextleftrec = new Record();
	nextrightrec = new Record();
	
	Pipe_Left.Remove(leftRecord);
	Pipe_Right.Remove(rightRecord);
	
	vector<Record*> left_similar_rec,right_similar_rec;
	int *atts_To_Keep = new int[ leftRecord->GetNumAtts() + rightRecord->GetNumAtts()];
	for(int i = 0 ; i < leftRecord->GetNumAtts();i++)
		atts_To_Keep[i] = i;
	for(int i = 0 ; i < rightRecord->GetNumAtts();i++)
		atts_To_Keep[ leftRecord->GetNumAtts()+i] = i;
	bool left_end = false , right_end = false;


	while(!(left_end||right_end))
	{
		int out = comp.Compare(leftRecord, LeftOrder, rightRecord, RightOrder); 
		if(out == 0)
		{
			left_similar_rec.push_back(leftRecord);
			while(1)
			{
				if(Pipe_Left.Remove(nextleftrec) == 0) {
					left_end = true;
					break;
				}
				if(comp.Compare(leftRecord, nextleftrec, LeftOrder) == 0)
				{
					left_similar_rec.push_back(nextleftrec);
					nextleftrec = new Record();
				}
				else
				{
					leftRecord = nextleftrec;
					nextleftrec = new Record();
					break;
				}
			}

			right_similar_rec.push_back(rightRecord);
			while(1)
			{
				if(Pipe_Right.Remove(nextrightrec) == 0)
				{ 
					right_end = true;
					break;
				}
				if(comp.Compare(rightRecord, nextrightrec, RightOrder) == 0)
				{
					right_similar_rec.push_back(nextrightrec);
					nextrightrec = new Record();
				}
				else
				{
					rightRecord = nextrightrec;
					nextrightrec = new Record();
					break;
				}
			}
			for(int i = 0; i < left_similar_rec.size(); i++)
			{
				for(int j = 0; j < right_similar_rec.size(); j++)
				{
					final_Record = new Record();
					final_Record->MergeRecords(left_similar_rec[i], right_similar_rec[j], left_similar_rec[i]->GetNumAtts(), right_similar_rec[j]->GetNumAtts(), atts_To_Keep, right_similar_rec[j]->GetNumAtts() + left_similar_rec[i]->GetNumAtts(), left_similar_rec[i]->GetNumAtts());
					run_j->out_Pipe->Insert(final_Record);
					//cout<<"j";
					count ++;
				}
			}
			left_similar_rec.clear();
			right_similar_rec.clear();

			if(left_end || right_end)
				break;
		}
		else {
			if(out < 0)
			{
				if(Pipe_Left.Remove(leftRecord) == 0){ 
					left_end = true;
					break;
				}
			}
			else
			{
				if(Pipe_Right.Remove(rightRecord) == 0) 
				{
					right_end = true;
					break;
				}
			}
		}
	}
	cout << "number of records inserted into pipe in Join: "<< count << endl;
	run_j->out_Pipe->ShutDown();
	return NULL;
}


void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) 
{
	inPipe_L = &inPipeL;
	inPipe_R = &inPipeR;
	out_Pipe = &outPipe;
	cnf = &selOp;
	literal_rec = &literal;
	numOfPages = 100;

	if(pthread_create(&thread, NULL, Join::Run_Join,(void *) this) != 0)
	{ 
		cerr << "Error in pthread creation" << endl; 
		return;
	}
}

void Join :: WaitUntilDone () 
{ 
	pthread_join(thread, NULL);
	cout << "Run_Join thread joined" << endl;
}
 
void Join :: Use_n_Pages (int n) 
{
	numOfPages = n; 
}

/////////////////////////////////////Dublicate Removal////////////////////////////////////////////////////

void* DuplicateRemoval::Run_DuplicateRemoval(void* arg){
	cout << "Run_DuplicateRemoval Started" << endl;
	int count = 0;
	int count2 = 0;
	DuplicateRemoval *run_dr;
	run_dr = (DuplicateRemoval *)arg;
	OrderMaker *order = new OrderMaker(run_dr->schema);

	Pipe temp(100);

	//BigQ *bq = new BigQ(*run_dr->in_Pipe,temp,*order,run_dr->runlength);
	BigQ bq(*run_dr->in_Pipe,temp,*order,run_dr->runlength);

	Record *temp_rec_1 = new Record();
	ComparisonEngine compare;
	Record *temp_rec_2 = new Record();
	//check for no records in Pipe
	temp.Remove(temp_rec_2);
	temp_rec_1->Copy(temp_rec_2);
	run_dr->out_Pipe->Insert(temp_rec_2);
	count ++;
	temp_rec_2->Copy(temp_rec_1);
	while(temp.Remove(temp_rec_1)){
		 if(compare.Compare(temp_rec_2,temp_rec_1,order)!=0){
			temp_rec_2->Copy(temp_rec_1);
			run_dr->out_Pipe->Insert(temp_rec_1);
			//cout<<"u";
			count ++;
		}
		
	}
	cout << endl << "total distinct records inserted into the pipe in Duplicate Removal: " << count << endl;
	run_dr->out_Pipe->ShutDown();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	in_Pipe = &inPipe;
	out_Pipe = &outPipe;
	schema = &mySchema;
	pthread_create (&thread, NULL, DuplicateRemoval::Run_DuplicateRemoval, this);
	
}

void DuplicateRemoval::WaitUntilDone (){	
	pthread_join (thread, NULL);
	cout<< "Run_DuplicateRemoval thread joined"<<endl;	
	
}

void DuplicateRemoval::Use_n_Pages (int n){

}

//////////////////////////////////////////////////// Sum ////////////////////////////////////////////////////////////////

void* Sum::Run_Sum(void* arg){
	cout<<"Run_Sum started"<<endl;	
	Sum *run_s;
	run_s = (Sum *)arg;
	Record pipe_rec;
	int integer = 0;
	int int_sum = 0;
	double dbl = 0;
	double double_sum = 0;
	Type type;
	Record outpipe_rec;
	while(run_s->in_Pipe->Remove(&pipe_rec)){
		type = run_s->func->Apply (pipe_rec, integer, dbl);
		if(type == Int){
			int_sum = int_sum+integer;
		}
		else if(type == Double){
			double_sum = double_sum+dbl;		
		}
	//cout<<"s";
	}
	if(type == Int){
		Attribute a_int;
		char *str = "new Attribute - column";
		a_int.name = str;
		a_int.myType = Int;
		stringstream ss;
		ss << int_sum;
		ss << "|";
		string str1 = ss.str();
		const char* final = str1.c_str();
		Schema *s = new Schema(NULL,1,&a_int);
		outpipe_rec.ComposeRecord (s, final); 
	}else if(type == Double){
		Attribute a_int;
		char *str = "new Attribute - column";
		a_int.name = str;
		a_int.myType = Double;
		stringstream ss;
		ss << double_sum;
		ss << "|";
		string str1 = ss.str();
		const char* final = str1.c_str();
		Schema *s = new Schema("temp",1,&a_int);
		outpipe_rec.ComposeRecord (s, final);
	}
	run_s->out_Pipe->Insert(&outpipe_rec);
	run_s->out_Pipe->ShutDown();	
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	in_Pipe = &inPipe;
	out_Pipe = &outPipe;
	func = &computeMe;
	pthread_create (&thread, NULL, Sum::Run_Sum, this);
	
}

void Sum::WaitUntilDone (){	
	pthread_join (thread, NULL);
	cout<< "Run_Sum thread Joined"<<endl;	
}

void Sum::Use_n_Pages (int n){

}

/////////////////////////////////////////////////GroupBy/////////////////////////////////////////////////////////////

void* GroupBy::Run_GroupBy(void* arg){
	cout<< "Run_GroupBy started"<<endl;		
	GroupBy *run_gb;
	run_gb = (GroupBy *)arg;
	int count = 0;
	Pipe temp(100);
	BigQ *bq = new BigQ(*run_gb->in_Pipe,temp,*run_gb->order,10);
	Record *in_Rec = new Record();

	ComparisonEngine ce;
	Record *previous = new Record();
	int output = temp.Remove(in_Rec);
	Type type;
	int num = 0;
	int int_sum = 0;
	double dbl = 0;
	double double_sum = 0;
	Record out_Rec;
	while(1){
		//cout<<"GB";
		if(output == 0) break;

		previous->Copy(in_Rec);
		type = run_gb->func->Apply(*in_Rec, num, dbl);
		if(type == Int){
			int_sum = int_sum+num;
		}
		else if(type == Double){
			double_sum = double_sum+dbl;
		}
		output = temp.Remove(in_Rec); 
					
		while(output==1 && ce.Compare(previous,in_Rec,run_gb->order)==0){
			type = run_gb->func->Apply(*in_Rec, num, dbl);
			if(type == Int){
				int_sum = int_sum+num;
			}
			else if(type == Double){
			double_sum = double_sum+dbl;		
			}
			output = temp.Remove(in_Rec); 	
		}

		if(type == Int){
		Attribute a_int;
		char *str = "new Attribute";
		a_int.name = str;
		a_int.myType = Int;
		stringstream ss;
		ss << int_sum;
		ss << "|";
		string str1 = ss.str();
		const char* final = str1.c_str();
		Schema *s = new Schema(NULL,1,&a_int);
		out_Rec.ComposeRecord (s, final); 
		}else if(type == Double){
		Attribute a_int;
		char *str = "new Attribute";
		a_int.name = str;
		a_int.myType = Double;
		stringstream ss;
		ss << double_sum;
		ss << "|";
		string str1 = ss.str();
		const char* final = str1.c_str();
		Schema *s = new Schema("temp",1,&a_int);
		out_Rec.ComposeRecord (s, final);
		}
		run_gb->out_Pipe->Insert(&out_Rec);
		count ++;
		int_sum = 0; double_sum = 0;
	}
	run_gb->out_Pipe->ShutDown();
	cout << "number of records inserted into pipe in GroupBy: "<< count << endl;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	in_Pipe = &inPipe;
	out_Pipe = &outPipe;
	func = &computeMe;
	order = &groupAtts;
	pthread_create (&thread, NULL, GroupBy::Run_GroupBy, this);
}

void GroupBy::WaitUntilDone () {
	pthread_join (thread, NULL);
	cout<< "Run_GroupBy thread joined"<<endl;
}

void GroupBy::Use_n_Pages (int n) { 

}

///////////////////////////////////////WriteOut////////////////////////////////////////////////


void* WriteOut::Run_WriteOut(void* arg){
	cout <<"Run_WriteOut started" << endl;
	WriteOut *run_wo;
	run_wo = (WriteOut *)arg;
	Record temp;
	while(run_wo->in_Pipe->Remove(&temp)){
		temp.PrintToFile (run_wo->schema,run_wo->out_File); 
	}
	fclose(run_wo->out_File);
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	in_Pipe = &inPipe;
	out_File = outFile;
	schema = &mySchema;
	pthread_create (&thread, NULL, WriteOut::Run_WriteOut, this);
}

void WriteOut::WaitUntilDone (){
	pthread_join (thread, NULL);
	cout << "Run_WriteOut thread Joined" << endl;
}

void WriteOut::Use_n_Pages (int n){

}
