#include "BigQ.h"
#include <vector>
#include <algorithm>
#include <pthread.h>
#include <time.h>
#include <sstream>

void *sort_stuff(void *arg);
/*struct CompareRecords{
	OrderMaker *mkr = new OrderMaker();
        ComparisonEngine *ce;
	bool operator()(Record *r1, Record *r2){

        int k = ce->Compare(r1, r2, mkr);
            return k>0;
        }
};*/
	//OrderMaker *globalorder = new OrderMaker();
	CompareRecords *cmpr;
	bool comparing(Record *i, Record *j){
	     ComparisonEngine *ce = new ComparisonEngine();
	     //if(ce->Compare(i,j,globalorder))
		//return true;
	     //else
		return false;
	}
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    // read data from in pipe sort them into runlen pages
    /*
    cmpr = new CompareRecords();
    cmpr -> mkr = &sortorder;
    cmpr -> ce = new ComparisonEngine();*/

    //globalorder = &sortorder;
    srand(time(NULL));
    thread_args *tp = new (std::nothrow) thread_args;
     tp->in = &in;
     tp->out = &out;
     tp->sortorder = &sortorder;
     tp->runlen = runlen;
     //g_sortOrder = &sortorder;
    pthread_t worker_thread;
    
    /*thread_args t_args = {&in, &out, &sortorder, runlen, cmpr};*/
    pthread_create(&worker_thread,NULL,sort_stuff,(void *)tp);
	
    // construct priority queue over sorted runs and dump sorted data 
     // into the out pipe
    //pthread_join(worker_thread,NULL);
    // finally shut down the out pipe
    //out.ShutDown ();
}

BigQ::~BigQ () {
}

void* sort_stuff( void *arg_in){
    cout<<"Inside bigq sort stuff"<<endl;
    thread_args *arg = (thread_args *)arg_in;
    File file;
    char ext_file_path[300];
    //srand(time(NULL));
    ostringstream convert;
    convert << rand()%500;
    string random_num = convert.str();
    // write the path for where the external should be created
    string new_file_text = "sortingfile";
    new_file_text.append(random_num);
    new_file_text.append(".txt");
    char *sorting_file = (char *)new_file_text.c_str();
    sprintf(ext_file_path,sorting_file);
    cout <<"sorting file name: " << sorting_file << endl;
    file.Open(0,ext_file_path);
    Page buffer_page;
    
    Page write_page;
	
    /*cmpr = new CompareRecords();
    cmpr -> sortorder = arg->sortorder;
    cmpr -> ce = new ComparisonEngine();*/

    Record *temp = new Record();
    int num_runs = 0; // stats from 0. num_run+1 is actual value
    int runlen = arg->runlen;
    std::vector<Record*> sort_buf;
    std::vector<run_data> run_info;
    int cur_page = 0;
    long page_length = 0;
    //int write_page_index = 0;

    run_data temp_data;
    temp_data.run_id = 0;
    temp_data.start_page = 0;
    //cout<<"About to enter while loop"<<endl;
    while(arg->in->Remove(temp)){
 	//cout << "Reading from input pipe BigQ.cc"<<endl;
        Record *rec_to_push_1 = new Record();
        Record *rec_to_push_2 = new Record();
        rec_to_push_1->Copy(temp);
        rec_to_push_2->Copy(temp);
        if(buffer_page.Append(rec_to_push_1)){
            //std::cout<< "Added to sort_buf"<<endl;
        }
        else{
            //std::cout<< "Buffer_page full" << endl;
            if(cur_page == (runlen-1)){
                // sort the vector
                //std::cout<< "Sorting the run"<<endl;
                write_page.EmptyItOut();
                std::sort(sort_buf.begin(),sort_buf.end(),CompareRecords(arg->sortorder));
                // write the vector to file, enter new run info into theinfo vector
                
                temp_data.run_id = num_runs;
                temp_data.start_page = page_length;
                //cout<<"temp_data.start_page "<<temp_data.start_page<<endl;
                //std::cout<< "Writing run to Disk"<<endl;
                for(int i=0;i<sort_buf.size();i++){
                    Record *rec_to_write_file_1 = new Record();
                    Record *rec_to_write_file_2 = new Record();
                    rec_to_write_file_1->Copy(sort_buf[i]);
                    rec_to_write_file_2->Copy(sort_buf[i]);
                    if(write_page.Append((Record *)rec_to_write_file_1)){
                        // Added Successfully
                    }
                    else{
                        
                        
                        file.AddPage(&write_page,page_length);
                        page_length++;

                        write_page.EmptyItOut();
                        if(write_page.Append((Record *)rec_to_write_file_2)){
                        }
                        else{
                            cout << "ERROR: While adding in an Empty Page 1!!" << endl;
                        }
                    }
                }
                file.AddPage(&write_page,page_length);
                temp_data.end_page = page_length;
                page_length++;
                run_data new_entry;
                new_entry.run_id = temp_data.run_id;
                new_entry.start_page = temp_data.start_page;
                new_entry.end_page = temp_data.end_page;
                run_info.push_back(new_entry);

                // empty_buffer page
                buffer_page.EmptyItOut();
                // curpage zero
                cur_page = 0;
                //empyt the vector
                sort_buf.clear();
                //increment number of runs
                num_runs++;
                //
                if(buffer_page.Append(rec_to_push_2) == 0){
                    cout << "ERROR: In Sort Stuff unable to append in cleaned page" << endl;
                }
            }
            else{
                buffer_page.EmptyItOut();
                if(buffer_page.Append(rec_to_push_2) == 0){
                    cout << "ERROR: In Sort Stuff unable to append in cleaned page" << endl;
                }
                cur_page++;
            }
        }
 	Record* push_into_vector = new Record();
	push_into_vector->Copy(temp);
        sort_buf.push_back(push_into_vector);
        //std::cout<< "Added to sort_buf"<<endl;
    }
    // putting the left over stuff in the new run

    //std::cout<< "Sorting the run"<<endl;
                write_page.EmptyItOut();
                sort(sort_buf.begin(),sort_buf.end(),CompareRecords(arg->sortorder));
                // write the vector to file, enter new run info into theinfo vector
                //Page write_page;
                //int real_run_length = 0;
                temp_data.run_id = num_runs;
                //temp_data.start_page = file.GetLength();
                /*if(file.GetLength()==0)
                    temp_data.start_page = 1;
                else
                        temp_data.start_page = file.GetLength();
                */
                temp_data.start_page = page_length;    
                //std::cout<< "Writing run to Disk"<<endl;
                for(int i=0;i<sort_buf.size();i++){
                    Record *rec_to_write_file_1 = new Record();
                    Record *rec_to_write_file_2 = new Record();
                    rec_to_write_file_1->Copy(sort_buf[i]);
                    rec_to_write_file_2->Copy(sort_buf[i]);
                    if(write_page.Append((Record *)rec_to_write_file_1)){
                        // Added Successfully
                    }
                    else{
                        file.AddPage(&write_page,page_length);
                        page_length++;
                        write_page.EmptyItOut();
                        if(write_page.Append((Record *)rec_to_write_file_2)){
                        }
            else{
                cout << "ERROR: While adding in an Empty Page 1!!" << endl;
                        }
                    }
                }
    file.AddPage(&write_page,page_length);
    temp_data.end_page = page_length;
    run_data new_entry;
    new_entry.run_id = temp_data.run_id;
    new_entry.start_page = temp_data.start_page;
    new_entry.end_page = temp_data.end_page;
    run_info.push_back(new_entry);
    // run generation done :)

    // merging begins
    //cout << "Merging begins in BigQ"<<endl;
    // varibles for mergestep. Array to find the min. Page Array to hold a page from each run.
    Record** record_heap = new Record*[num_runs+1];
    for(int i=0;i<(num_runs+1);i++){
        record_heap[i] = new Record();
    }
    bool exhausted_flag[num_runs+1];
    Record* null_record; //as a replacementfor null
    Page** run_page_holder = new Page*[num_runs+1];
    for(int i=0;i<(num_runs+1);i++){
        run_page_holder[i] = new Page();
    }
    // initialze the run_page_holder and the record_heap
    for(int i=0;i<(num_runs+1);i++){
        file.GetPage(run_page_holder[i],run_info[i].start_page);
        run_info[i].cur_page = run_info[i].start_page;
        run_page_holder[i]->GetFirst(record_heap[i]);
        exhausted_flag[i] = false;
    }// initalization done

    ComparisonEngine comp_e;
    long count = 0;
    while(true){
        //bool exit_now = false;
        int num_nulls = 0; // to checkif all elems inheap are null. if so exit the loop
        Record *min_record;
        int min_index = 0;
        min_record = record_heap[0];
        for(int i=0;i<(num_runs+1);i++){
            //if(record_heap[i] == NULL){
            if(exhausted_flag[i] == true){
                num_nulls++;
        
                continue;
            }
            else if(exhausted_flag[min_index] == true){
                min_record = record_heap[i];
                min_index = i;
            }
            if((comp_e.Compare(min_record,record_heap[i],arg->sortorder)) > 0){
	    //if((comp_e.Compare(min_record,record_heap[i],arg->sortorder)) < 0){
                min_record = record_heap[i];
                min_index = i;
            }
        }
        // if all are null then exit from the loop and the sorting is done
        if(num_nulls == (num_runs+1)){
            cout << "Sorting Done! BigQ.cc"<< endl;
            break; // exits the loop
        }
    	count++;
	Record *insert_rec = new Record();
	insert_rec->Copy(min_record);
	//insert_rec->Print();
        arg->out->Insert(insert_rec);// Insert into ouput pipe
	//cout << "writing to out pipe BigQ.cc" << endl;
        // reinitialize the record_heap wirh new record
        // if page ends load new page. If its the last page put null into the record_heap
        if(run_page_holder[min_index]->GetFirst(record_heap[min_index]) == 1){
            // loaded the new record successfully
        }
        else{
            if(run_info[min_index].cur_page == run_info[min_index].end_page){
                //record_heap[min_index] = NULL; // the run has been exhausted
                exhausted_flag[min_index] = true;
            }
            else{
                run_info[min_index].cur_page++;
                file.GetPage(run_page_holder[min_index],run_info[min_index].cur_page);
                if(run_page_holder[min_index]->GetFirst(record_heap[min_index]) == 1){
                    // loaded the new record successfully
                }
                else{
                    cout << "ERROR loading record from new page"<< endl;
                }        
            }
//arg->out->ShutDown();
        }
    }
file.Close();
arg->out->ShutDown();
//cout<<"BigQ work done"<<endl;
}
