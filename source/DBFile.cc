#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <stdio.h>
#include <cstring>
#include <fstream>
#include "BigQ.h"

// stub file .. replace it with your own DBFile.cc

//void* bigq_create(void* args_in);

DBFile::DBFile () {
	flag = 0;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {

	file.Open(0,f_path);
	type = f_type;
	modified = false;
	current_page_index = 0;
	buffer_page = new Page();
	operation_state = reading;

	// Creating the metafile
	sort_input *input_params = (sort_input *)startup;
	int i;
	//char meta_path[100];
	for(int k=0;k<100;k++){
		file_path[k] = f_path[k];
	}
	for(i=0; i<100;i++){
		meta_path[i] = f_path[i];
		if(f_path[i]=='\0')
			break;
	}
	// apple.bin i=9
	meta_path[i-4]='_';
	meta_path[i-3]='m';
	meta_path[i-2]='.';
	meta_path[i-1]='t';
	meta_path[i]='x';
	meta_path[i+1]='t';
	meta_path[i+2]='\0';
	ofstream meta_file;
	meta_file.open(meta_path);
	if(f_type == heap){
 		meta_file << "h";
	}
	else{
		meta_file << "s"<<input_params->o->numAtts<<endl;
		run_length = input_params->l;
		meta_file << run_length << endl;	
		for(int j=0;j<input_params->o->numAtts;j++){
			meta_file << input_params->o->whichAtts[j] << " " << input_params->o->whichTypes[j]<<endl;
		}
 	}
 	meta_file.close();
	return 1;
}

int DBFile::WriteBufferToDisk(){
	file.AddPage(buffer_page,current_page_index);
	return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	Record temp;
	FILE *tablefile = fopen(loadpath,"r");
	int counter=0;
	while (temp.SuckNextRecord (&f_schema, tablefile) == 1) {
		counter++;
		Add(temp);
	}
	operation_state = writing;
}

int DBFile::Open (char *f_path) {
	file.Open(1,f_path);
	//cout << "file length in pages:   " << file.GetLength()<<"  in func open at top   DBFile.cc" << endl;
	operation_state = reading;
	for(int k=0;k<100;k++){
		file_path[k] = f_path[k];
	}
 	//opening metafile
 	int i;
	//char meta_path[100];
	for(i=0; i<100;i++){
		meta_path[i] = f_path[i];
		if(f_path[i]=='\0')
			break;
	}
	// apple.bin i=9
	meta_path[i-4]='_';
	meta_path[i-3]='m';
	meta_path[i-2]='.';
	meta_path[i-1]='t';
	meta_path[i]='x';
	meta_path[i+1]='t';
	meta_path[i+2]='\0';
	ifstream  meta_read_file;
	meta_read_file.open(meta_path);
	char type_in;
		
	meta_read_file >> type_in;
	if(type_in=='s'){
		cout << "file length in pages - Open:   " << file.GetLength()<<"  TYPE: SORTED" << endl;	
		type = sorted;
		order = new OrderMaker();
		meta_read_file >> order->numAtts;
		meta_read_file >> run_length;

		for(int i=0;i<order->numAtts;i++){
			int att,type;
			meta_read_file >> att >> type;
			order->whichAtts[i] = att;
			order->whichTypes[i] = static_cast <Type>(type);
		}
	}
	else{
		type = heap;
		cout << "file length in pages - Open :   " << file.GetLength()<<"  Type : HEAP" << endl;	
	}
		
	
	return 1;
}

void DBFile::MoveFirst () {
	//Page *first_page;
	cout << "file length in pages - Movefirst: " << file.GetLength()<< "  at top of vun Movefirst " << endl;
	if(operation_state == writing){
		if(type == heap)
			WriteBufferToDisk();
		else{
			// call the sort and write function
			sort_merge();
		}
	}
	operation_state = reading;
	cur_page = new Page();
	file.GetPage(cur_page,0);
	current_page_index = 0;
	//cout << "file length in pages Movefirst: " << file.GetLength()<< "  at bottom of vun Movefirst " << endl;

}

int DBFile::Close () {
	if(operation_state == writing){
		if(type == heap)
			WriteBufferToDisk();
		else{
			// call the sort and write function
			sort_merge();
		}
	}

	int i = file.Close();
	
	return i;
}

void *bigq_create(void* args_in){
	
	thread_arg_123 *inParams = (thread_arg_123 *) args_in;
	BigQ bq(*inParams->inPipe, *inParams->outPipe, *inParams->sortOrder,
			inParams->runLen);
}

void DBFile::Add (Record &rec) {
	if(type == heap){
		operation_state = writing;
		Record *rec_to_push = new Record();
        	rec_to_push->Copy(&rec);
		if(buffer_page->Append(&rec)){
			file.AddPage(buffer_page,current_page_index);
			return;
		}
		else{
			file.AddPage(buffer_page,current_page_index);
			current_page_index++;
			buffer_page->EmptyItOut();
			buffer_page->Append(rec_to_push);
			return;
		}
	}
	else{
		//Record to_remove;
		
		if(operation_state == reading){
			// setup bigq class;
			operation_state = writing;
			if(flag ==0){
				input = new Pipe(100);
				output = new Pipe(100);
				cout << "bigQ spawning" << endl;
			pthread_t bigq_spawn_thread;
			
			thread_arg_123 *threadargs = new (std::nothrow) thread_arg_123;
			threadargs->inPipe = input;
			threadargs->outPipe = output;
			threadargs->sortOrder = order;
			threadargs->runLen = run_length;
			input->Insert(&rec);
			pthread_create(&bigq_spawn_thread,NULL,bigq_create,(void *)threadargs);
  			cout << "bigQ thread spawned" << endl;
  			flag = 1;
			}
 			
		}
		else if(operation_state == writing){
			input->Insert(&rec);
		}
		//write to pipe
	}
}



int DBFile::sort_merge(){
	//create tempfile
	int num_file_read =0, num_file_write =0,num_pipe_read =0, num_pipe_write =0, num_compares=0;
	input->ShutDown();
	DBFile temp_file;
	char temp_file_path[100];
	int i;
	for(i=0; i<100;i++){
		temp_file_path[i] = meta_path[i];
		if(meta_path[i]=='\0')
			break;
	}
	temp_file_path[i-5] = 'z';
	temp_file.Create(temp_file_path,heap,NULL);

	if(file.GetLength() == 0){
	// move current file to start
	operation_state = reading;
	Record pipe_rec;
		while(output->Remove(&pipe_rec)){
			num_pipe_read++;			
			Record rec_to_add;
			rec_to_add.Copy(&pipe_rec);
			temp_file.Add(rec_to_add);
			num_pipe_write++;	
		}
	}
	else {
	// move current file to start
	operation_state = reading;
	//temp_file.MoveFirst();	
	//cout << "temp_file move first done" << endl;	
	MoveFirst();
	cout << "current file move first done" << endl;
	Record pipe_rec, file_rec;
	int file_empty_flag = 1;
	ComparisonEngine comp_eng;
	int check =2;
	last_written last_added = file_r; 
	// read stuff from outpipe
	while(output->Remove(&pipe_rec)){
		// runs if there is stuff in the pipe
		file_empty_flag = 1;
		num_pipe_read++;
		while(file_empty_flag){
			if(last_added == file_r){
				num_file_read++;
				check = GetNext(file_rec);
				
			}
			if(check == 0){
				Record rec_to_add;
				rec_to_add.Copy(&pipe_rec);
				temp_file.Add(rec_to_add);
				num_pipe_write++;
				last_added = pipe_r;
				file_empty_flag = 0;
			}
			else{
				
				//compare and write the smlllest
				num_compares++;
				if(comp_eng.Compare(&pipe_rec,&file_rec,order) > 0){
					// piperec is > filerec
					Record rec_to_add;
					rec_to_add.Copy(&file_rec);
					temp_file.Add(rec_to_add);
					num_file_write++;
					last_added = file_r;
				}
				else{
					Record rec_to_add;
					rec_to_add.Copy(&pipe_rec);
					temp_file.Add(rec_to_add);
					num_pipe_write++;
					last_added = pipe_r;
					file_empty_flag = 0;
				}
			}
		}
	}
	if(check == 1){
		Record rec_to_add;
		rec_to_add.Copy(&file_rec);
		temp_file.Add(rec_to_add);
		num_file_write++;
}
	while(GetNext(file_rec)){
		num_file_read++;
		Record rec_to_add_1;
		rec_to_add_1.Copy(&file_rec);
		temp_file.Add(rec_to_add_1);
		num_file_write++;
	}
	cout << "all records added " << endl;
	}
	int size = temp_file.Close();
	file.Close();
	cout << "file length in pages: " << size << endl;
	if(remove(file_path) == 0){
		cout << "File removed: "<<file_path<<endl;
	}
	else{
		cout << "File remove failed: "<<file_path<<endl;
	}	
	if(rename(temp_file_path,file_path) == 0){
		cout<< "File Renamed from: "<<temp_file_path<<"  to  :"<<file_path<<endl;
	}
	else{
		cout<< "File Renamed failed!! from: "<<temp_file_path<<"  to  :"<<file_path<<endl;
	}
	Open(file_path);
	MoveFirst();
	cout << "Merging Done" << endl;
	return 0;
}

int DBFile::GetNext (Record &fetchme) {
	//cout<<"GetNext called DBFIle.cc Getnext"<<endl;
	//cout << " file length getnext(): " << file.GetLength() << endl;
	if(operation_state == writing){
		if(type == heap)
			WriteBufferToDisk();
		else{
			// call the sort and write function
			sort_merge();
		}
	}
	if(cur_page->GetFirst(&fetchme)){
		return 1;
	}
	else{
		if(current_page_index == (file.GetLength()-2)){
			return 0;
		}
		else{
			current_page_index++;
			file.GetPage(cur_page,current_page_index);
			return cur_page->GetFirst(&fetchme);
		}
	}
	operation_state = reading;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//cout << " file length GETNEXT(CNF): " << file.GetLength() << endl;
	if(operation_state == writing){
		if(type == heap)
			WriteBufferToDisk();
		else{
			// call the sort and write function
			sort_merge();
		}
	}
	Record* temp = new Record();
	ComparisonEngine* comp = new ComparisonEngine();
	while(true){
		if(GetNext(*temp)){
			if (comp->Compare (temp, &literal, &cnf)){
				fetchme = *temp;
				return 1;
			}	
		}
		else{
			std::cout<< "Found Last one  DBFile.cc GetNext" << endl;
			return 0;
		}
	}
	operation_state = reading;
}
