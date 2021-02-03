#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include <string>
#include <iostream>
#include <map>
#include <iterator>
#include <fstream>
#include <cstring>
#include <algorithm>
#include <numeric>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdlib.h>

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using masterworker::WorkerTask;
using masterworker::WorkerReply;
using masterworker::Masterworker;
using masterworker::ShardInfo;


/*
*  
*  cout << "\033[1;31mbold red text\033[0m\n";
*  Here, \033 is the ESC character, ASCII 27. It is followed by [, then zero or more numbers 
*  separated by ;, and finally the letter m. The numbers describe the colour and format to 
*  switch to from that point onwards.
*  
*  The codes for foreground and background colours are:
*  
*           foreground background
*  black        30         40
*  red          31         41
*  green        32         42
*  yellow       33         43
*  blue         34         44
*  magenta      35         45
*  cyan         36         46
*  white        37         47
*  Additionally, you can use these:
*  
*  reset             0  (everything back to normal)
*  bold/bright       1  (often a brighter shade of the same colour)
*  underline         4
*  inverse           7  (swap foreground and background colours)
*  bold/bright off  21
*  underline off    24
*  inverse off      27
*  
*/
//the following are UBUNTU/LINUX, and MacOS ONLY terminal color codes.
#define RESET   "\033[0m"
#define BLACK   "\033[30m"      /* Black */
#define RED     "\033[31m"      /* Red */
#define GREEN   "\033[32m"      /* Green */
#define YELLOW  "\033[33m"      /* Yellow */
#define BLUE    "\033[34m"      /* Blue */
#define MAGENTA "\033[35m"      /* Magenta */
#define CYAN    "\033[36m"      /* Cyan */
#define WHITE   "\033[37m"      /* White */
#define BOLDBLACK   "\033[1m\033[30m"      /* Bold Black */
#define BOLDRED     "\033[1m\033[31m"      /* Bold Red */
#define BOLDGREEN   "\033[1m\033[32m"      /* Bold Green */
#define BOLDYELLOW  "\033[1m\033[33m"      /* Bold Yellow */
#define BOLDBLUE    "\033[1m\033[34m"      /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m"      /* Bold Magenta */
#define BOLDCYAN    "\033[1m\033[36m"      /* Bold Cyan */
#define BOLDWHITE   "\033[1m\033[37m"      /* Bold White */
#define BGBLACK   "\033[40m"      /* Background Black */
#define BGRED     "\033[41m"      /* Background Red */
#define BGGREEN   "\033[42m"      /* Background Green */
#define BGYELLOW  "\033[43m"      /* Background Yellow */
#define BGBLUE    "\033[44m"      /* Background Blue */
#define BGMAGENTA "\033[45m"      /* Background Magenta */
#define BGCYAN    "\033[46m"      /* Background Cyan */
#define BGWHITE   "\033[47m"      /* Background White */




/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */

class WorkerData{
	
	std::string worker_ip_addr;
	int mr_id_holder;

	std::shared_ptr<Channel> channel;
	std::unique_ptr<Masterworker::Stub> stub_;
		
	CompletionQueue* cq_;

	WorkerTask request;
	WorkerReply reply;

    ClientContext context;
    Status status;

	std::unique_ptr<ClientAsyncResponseReader<WorkerReply>> response_reader;
	

	public:
		
		WorkerData(std::string &ip_addr, CompletionQueue* cq);		

		//~WorkerData(){ delete this;}

		inline std::string get_ip_addr(){ return worker_ip_addr; }
		inline int get_reply_id(){ return reply.id(); }
		inline std::string get_reply_status(){ return reply.status(); }
		inline bool isStatusOk(){ return status.ok(); }

		inline int get_mr_id(){ return mr_id_holder; }

		void destroy(){ delete this; }

		// method to populate the request.
		//void assembleRequest(std::string &worker_type, int &mr_id, std::string &in_dirpath, std::string &out_dirpath, std::string &userid, int &n_outfiles, std::map <std::string, std::pair <std::streampos,std::streampos> > &shardInfo);
		void assembleRequest(std::string worker_type, int mr_id, std::string in_dirpath, std::string out_dirpath, std::string userid, int n_outfiles, FileShard& shard);

	   // send message to server.
		void asyncSendTaskToWorker();
};


void WorkerData::asyncSendTaskToWorker(){

	this->response_reader = this->stub_->PrepareAsyncSendWorkerTask( &(this->context), this->request, cq_);
	this->response_reader->StartCall();
	this->response_reader->Finish( &(this->reply), &(this->status), this);

}

WorkerData::WorkerData(std::string &ip_addr, CompletionQueue* cq):worker_ip_addr(ip_addr), cq_(cq){
	this->channel = grpc::CreateChannel(worker_ip_addr, grpc::InsecureChannelCredentials());
	this->stub_   = Masterworker::NewStub(this->channel);
}

//void WorkerData::assembleRequest(std::string &worker_type, int &mr_id, std::string &in_dirpath, std::string &out_dirpath, std::string &userid, int &n_outfiles, std::map <std::string, std::pair <std::streampos,std::streampos> > &shardInfo ){
void WorkerData::assembleRequest(std::string worker_type, int mr_id, std::string in_dirpath, std::string out_dirpath, std::string userid, int n_outfiles, FileShard& shard ){
		//FileShard shard;
		this->mr_id_holder = mr_id;

		this->request.set_workertype(worker_type);
		this->request.set_id(mr_id);
		this->request.set_in_dirpath(in_dirpath);
		this->request.set_out_dirpath(out_dirpath);
		this->request.set_userid(userid);
		this->request.set_n_outfiles(n_outfiles);

		ShardInfo* shardEntry;
		std::map <std::string, std::pair <std::streampos,std::streampos> >::iterator itr;		

		for(itr=shard.shardInfo.begin(); itr!=shard.shardInfo.end(); ++itr){ 
			//std::cout<<"Inside thFilename: "<<itr->first<<"   start_pos: "<<(itr->second).first<<"  End pos: "<<(itr->second).second<<std::endl;
			
			//printf("Filename: %30s  start_pos: %-10d  end_pos: %-10d\n")			

			shardEntry = this->request.add_shards();

			shardEntry->set_filepath(itr->first);
			shardEntry->set_start_pos((itr->second).first);
			shardEntry->set_end_pos((itr->second).second);
		
			int size = (itr->second).second - (itr->second).first;
			shardEntry->set_size(size);

		}
}


enum WorkerStatus{ AVAILABLE, BUSY, FAILED };

struct WorkerInfo{
	std::string ip_addr;
	WorkerStatus status_;
};


class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec localSpecCopy;
	
		std::vector<FileShard> localFileShardCopy;

		std::vector<WorkerInfo> workers;

		std::vector<int> failed_tasks;

		CompletionQueue cq_;

		inline int getNumFailedWorkers(){
			int count = 0;		
			for(auto &x: workers){
				if(x.status_== FAILED){
					++count;
				}
			}
			return count;
		}

};




/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {

	// Popoulate the local copy of the mr_spec for master.
	localSpecCopy.workers.clear();
	localSpecCopy.workers.assign(mr_spec.workers.begin(),mr_spec.workers.end());
	
	localSpecCopy.inputFiles.clear();
	localSpecCopy.inputFiles.assign(mr_spec.inputFiles.begin(),mr_spec.inputFiles.end());

	localSpecCopy.numWorkers     = mr_spec.numWorkers;
	localSpecCopy.numOutputFiles = mr_spec.numOutputFiles;

	localSpecCopy.outDirName    = mr_spec.outDirName;	
	localSpecCopy.shardSizeInKB = mr_spec.shardSizeInKB;

	localSpecCopy.userID        = mr_spec.userID;	


	// Populatin the local fileshards vector for master.
	
	localFileShardCopy.clear();
	localFileShardCopy.assign(file_shards.begin(),file_shards.end());

	int count=0;
	std::cout<<BOLDCYAN<<"[INFO]: Sharding input Files! \n"<<RESET<<std::endl;
	for(auto &x: localFileShardCopy){
		std::cout<<BOLDBLUE<<"        shard "<<RESET<<count<<":"<<std::endl;
		std::map <std::string, std::pair <std::streampos,std::streampos> >::iterator itr;
		for(itr=x.shardInfo.begin(); itr!=x.shardInfo.end(); ++itr){
			//std::cout<<"Inside the request !  Filename: "<<itr->first<<"   start_pos: "<<(itr->second).first<<"  End pos: "<<(itr->second).second<<std::endl;
			std::string filename = itr->first;
			int start_pos         = (itr->second).first;
			int end_pos           = (itr->second).second;

			printf("                 \033[38;5;200mFilename:\033[0m %-25s      \033[38;5;200mstart_pos:\033[0m %7d      \033[38;5;200mend_pos:\033[0m %7d      \033[38;5;200msize (in Bytes):\033[0m %7d\n",filename.c_str(),start_pos,end_pos,end_pos-start_pos);
		}
		std::cout<<std::endl;
		++count;
	}
	std::cout<<std::endl;

	// populate the workers vector.
	for(auto &addr : localSpecCopy.workers){
		WorkerInfo info;
		info.ip_addr = addr;  
		info.status_ = AVAILABLE;
		workers.push_back(info);
	}

}



/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	std::cout<<"\n[INFO]:"<<BOLDCYAN<<"\t\tStarting the Map phase!"<<RESET<<std::endl;
	
	std::string path,cmd;
	cmd = "rm -rf intermediate";

	if(system(cmd.c_str()) == -1){
		std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tError! Cannot delete directory: ./intermediate "<<RESET<<cmd<<std::endl;
	}else{
		std::cout<<"\n[INFO]:\tDirectory "<<BOLDMAGENTA<<"./intermediate"<<RESET<<" deleted! (command): "<<BOLDMAGENTA<<cmd<<RESET<<std::endl<<std::endl;
	}

	std::cout<<"[INFO]:\tCreating the folders for intermediate results: "<<std::endl;
	// Create the intermediate directories for mappers outputs.
	for(int i=0; i<localSpecCopy.numOutputFiles; ++i){

		path = "intermediate/folder_" + std::to_string(i);
		cmd = "mkdir -p "+path;
		
		if(system(cmd.c_str()) == -1){
			std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tError! Cannot create directory: "<<RESET<<path<<std::endl;
		}else{
			std::cout<<"[INFO]:\tDirectory created: "<<BOLDMAGENTA<<path<<RESET<<std::endl;
		}
	}

	std::cout<<std::endl;



	int availableWorkers = workers.size();

	for(int i=0; i<localFileShardCopy.size(); ++i){

		if(availableWorkers>0){
			for(auto &worker: workers){
				if(worker.status_== AVAILABLE){

					WorkerData* task = new WorkerData(worker.ip_addr,&cq_);
				
					task->assembleRequest("mapper",i,"dummy","intermediate", localSpecCopy.userID, localSpecCopy.numOutputFiles, localFileShardCopy[i]);

					task->asyncSendTaskToWorker();

					std::cout<<"\n[INFO]:"<<BLUE<<" Sent RPC to Worker: "<<RESET<<worker.ip_addr<<"   Mapper_ID: "<<i<<std::endl<<std::endl;

					// update the status of the worker->
					worker.status_ = BUSY;

					// Decrement the available number of workers
					--availableWorkers;

					// We found one available worker and assigned a task. so break out!
					break;
				}				
			}
		}
		else{

			// reset the count of i here.
			--i;

			void* got_tag;
        	bool ok = false;

			GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call; Waiting for someone to respond!
       	    GPR_ASSERT(ok);

			std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();

			for(auto &worker: workers){

				if(ip_addr.compare(worker.ip_addr)==0){
					
					//GPR_ASSERT(got_tag == worker->tag);

           		 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){
            			//std::cout << "RPC successful from Mapper: "<< static_cast<WorkerData*>(got_tag)->get_reply_id() << " Status: " << static_cast<WorkerData*>(got_tag)->get_reply_status()<<std::endl;
						std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
						int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
						std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
					
						std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Mapper_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;

						// update the status of worker 
						worker.status_ = AVAILABLE;

						// Since we received a succesful reply back, 
						// increment the available number of workers.
						++availableWorkers;
					}
            	 	else{
						std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
						std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

						worker.status_ = FAILED;
						// Since this a failed worker, we need to 
						// redo the work for this mapper. So track this id
						// in the failed vector.
						failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());		
					}
				
					static_cast<WorkerData*>(got_tag)->destroy();

					// we found our guy! break out!
					break;
				}
			}
		}	
	}

	// Look out of the remaining mappers.
	while( availableWorkers != ( workers.size() - getNumFailedWorkers() )  ){

		//std::cout<<"avaiableWorkers: "<<availableWorkers<<" busy: "<<getNumBusyWorkers()<<std::endl;

		void* got_tag;
        bool ok = false;

		GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call;
       	GPR_ASSERT(ok);

		std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
		for(auto &worker: workers){

			if(ip_addr.compare(worker.ip_addr)==0){
				
				//GPR_ASSERT(got_tag == worker->tag);

        	 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

					std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
					int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
					std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
					
					std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Mapper_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;

					// update the status of worker 
					worker.status_ = AVAILABLE;

					// Since we received a succesful reply back, 
					// increment the available number of workers.
					++availableWorkers;
				}
        	 	else{
					std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
					std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;
					
					worker.status_ = FAILED;
					// Since this a failed worker, we need to 
					// redo the work for this mapper. So track this id
					// in the failed vector.
					failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());				

				}

				static_cast<WorkerData*>(got_tag)->destroy();

				// we found our guy! break out!
				break;
			}
		}

	}


	int numTasks = localFileShardCopy.size();
	int numFail  = failed_tasks.size();
	int numPass  = numTasks - numFail;

	std::cout << "\n\n\n[INFO]: \t"<<BOLDCYAN<<"MAP PHASE SUMMARY:"<<BOLDMAGENTA<<"     TOTAL MAP TASKS RUN: "<<numTasks<<BOLDGREEN<<"    PASS: "<<numPass<<BOLDRED<<"    FAIL: "<<numFail<<RESET<<std::endl<<std::endl;

	if(numFail == numTasks){
		std::cout << "\n\n[ERROR]:"<<BOLDRED<<"\t\tNo running workers available! Exiting MAP Phase ..."<<RESET<<std::endl<<std::endl;
		return false;	
	}


	if(failed_tasks.size()){
		std::cout << "\n\n[INFO]:"<<BOLDCYAN<<"\t\tRedoing the failed tasks from MAP Phase!"<<RESET<<std::endl<<std::endl;
	}
	// Redo the failed tasks here.

	while( failed_tasks.size() ){

			if(availableWorkers>0){
				for(auto &worker: workers){
					if(worker.status_== AVAILABLE){

						WorkerData* task = new WorkerData(worker.ip_addr,&cq_);
										
						int id = failed_tasks.front(); 
						failed_tasks.erase(failed_tasks.begin());

						task->assembleRequest("mapper",id,"dummy","intermediate", localSpecCopy.userID, localSpecCopy.numOutputFiles, localFileShardCopy[id]);
						task->asyncSendTaskToWorker();

						std::cout<<"\n[INFO]:"<<BLUE<<" Sent RPC to Worker: "<<RESET<<worker.ip_addr<<"   Mapper_ID: "<<id<<std::endl<<std::endl;

						// update the status of the worker->
						worker.status_ = BUSY;

						// Decrement the available number of workers
						--availableWorkers;

						// We found one available worker and assigned a task. so break out!
						break;
					}				
				}
			}
			else{

				void* got_tag;
        		bool ok = false;

				GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call; Waiting for someone to respond!
       		    GPR_ASSERT(ok);

				std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();

				for(auto &worker: workers){

					if(ip_addr.compare(worker.ip_addr)==0){
						
						//GPR_ASSERT(got_tag == worker->tag);

        	   		 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

						std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
						int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
						std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
						
						std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Mapper_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;

						// update the status of worker 
						worker.status_ = AVAILABLE;

						// Since we received a succesful reply back, 
						// increment the available number of workers.
						++availableWorkers;
						}
        	    	 	else{
							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
							std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

							// Since this a failed worker, we need to 
							// redo the work for this mapper. So track this id
							// in the failed vector.
							failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());		
						}
					
						static_cast<WorkerData*>(got_tag)->destroy();

						// we found our guy! break out!
						break;
					}
				}
			}

			// Look out of the remaining mappers.
			while( availableWorkers != ( workers.size() - getNumFailedWorkers() )  ){
				void* got_tag;
    		    bool ok = false;

				GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call;
    		   	GPR_ASSERT(ok);

				std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
				for(auto &worker: workers){

					if(ip_addr.compare(worker.ip_addr)==0){
						
						//GPR_ASSERT(got_tag == worker->tag);

    		    	 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
							int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
							std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
							
							std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Mapper_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;

							// update the status of worker 
							worker.status_ = AVAILABLE;

							// Since we received a succesful reply back, 
							// increment the available number of workers.
							++availableWorkers;
						}
    		    	 	else{
							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
							std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

							// Since this a failed worker, we need to 
							// redo the work for this mapper. So track this id
							// in the failed vector.
							failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());				

						}

						static_cast<WorkerData*>(got_tag)->destroy();

						// we found our guy! break out!
						break;
					}
				}

			}
	}


	std::cout << "\n[INFO]:"<<BOLDCYAN<<"\t\tCompleted the Map phase! "<<RESET<<std::endl<<std::endl;


	/*
	// Before going into the reduce phase, clear all the failed_tasks vector
	failed_tasks.clear();




	for(auto&x: workers){
		x.status_ = AVAILABLE;
	}

	availableWorkers = workers.size();

	*/



	// -------------------------  Do the reducer part here  -------------------------------


	// Create the output folder for reducers outputs.
	std::cout<<"\n\n\n[INFO]:"<<BOLDCYAN<<"\t\tStarting the Reduce phase!"<<RESET<<std::endl;

	cmd = "rm -rf output";

	if(system(cmd.c_str()) == -1){
		std::cout<<"\n[ERROR]: Error! Cannot delete directory: ./output "<<cmd<<std::endl;
	}else{
		std::cout<<"\n[INFO]:\tDirectory "<<BOLDMAGENTA<<"./output"<<RESET<<" deleted! (command): "<<BOLDMAGENTA<<cmd<<RESET<<std::endl<<std::endl;
	}

	if(localSpecCopy.outDirName.compare("") == 0){
		cmd = "mkdir output";
	}else{
		cmd = "mkdir "+localSpecCopy.outDirName;
	}

	std::cout<<"[INFO]:\tCreating the folder ./"<<BOLDMAGENTA<<localSpecCopy.outDirName<<RESET<<" for reducers output: "<<std::endl;

	if(system(cmd.c_str()) == -1){
		std::cout<<"\n[ERROR]:\tError! Cannot create directory ./ "<<localSpecCopy.outDirName<<std::endl;
	}else{
		std::cout<<"[INFO]\tDirectory "<<BOLDMAGENTA<<"./"<<localSpecCopy.outDirName<<RESET<<" created! "<<std::endl;
	}


	for(int i=0; i<localSpecCopy.numOutputFiles; ++i){

		if(availableWorkers>0){
			for(auto &worker: workers){
				if(worker.status_== AVAILABLE){


					WorkerData* task = new WorkerData(worker.ip_addr,&cq_);

					std::string path = "intermediate/folder_" + std::to_string(i);

					task->assembleRequest("reducer",i,path,"output", localSpecCopy.userID, localSpecCopy.numOutputFiles, localFileShardCopy[0]);	
					task->asyncSendTaskToWorker();

					std::cout<<"\n[INFO]:"<<BLUE<<" Sent RPC to Worker: "<<RESET<<worker.ip_addr<<"   Reducer_ID: "<<i<<std::endl<<std::endl;

					// update the status of the worker->
					worker.status_ = BUSY;

					// Decrement the available number of workers
					--availableWorkers;

					// We found one available worker and assigned a task. so break out!
					break;
				}				
			}
		}
		else{

			// reset the count of i here.
			--i;

			void* got_tag;
        	bool ok = false;

			GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call; Waiting for someone to respond!
       	    GPR_ASSERT(ok);

			std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();

			for(auto &worker: workers){

				if(ip_addr.compare(worker.ip_addr)==0){
					
					//GPR_ASSERT(got_tag == worker->tag);

           		 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

						std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
						int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
						std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
					
						std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Reducer_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;

						// update the status of
						worker.status_ = AVAILABLE;

						// Since we received a reply back, 
						// increment the available number of workers.
						++availableWorkers;
					}
            	 	else{
						std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
						std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

						worker.status_ = FAILED;
						// Since this a failed worker, we need to 
						// redo the work for this mapper. So track this id
						// in the failed vector.
						failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());		
					}
				
					static_cast<WorkerData*>(got_tag)->destroy();

					// we found our guy! break out!
					break;
				}
			}
		}	
	}


	// Look out of the remaining mappers.
	while( availableWorkers != workers.size() - getNumFailedWorkers()){
		void* got_tag;
        bool ok = false;

		GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call;
       	GPR_ASSERT(ok);

		std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
		for(auto &worker: workers){

			if(ip_addr.compare(worker.ip_addr)==0){
				
				//GPR_ASSERT(got_tag == worker->tag);

        	 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

					std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
					int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
					std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
					
					std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Reducer_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;
					
					// update the status of
					worker.status_ = AVAILABLE;

					// Since we received a reply back, 
					// increment the available number of workers.
					++availableWorkers;
				}
        	 	else{
					std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
					std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

					worker.status_ = FAILED;

					// Since this a failed worker, we need to 
					// redo the work for this mapper. So track this id
					// in the failed vector.
					failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());		
				}
				

				static_cast<WorkerData*>(got_tag)->destroy();

				// we found our guy! break out!
				break;
			}
		}

	}



	numTasks = localSpecCopy.numOutputFiles;
	numFail  = failed_tasks.size();
	numPass  = numTasks - numFail;

	std::cout << "\n\n\n[INFO]:"<<BOLDCYAN<<"\tREDUCE PHASE SUMMARY:"<<BOLDWHITE<<"     TOTAL REDUCE TASKS RUN: "<<numTasks<<BOLDGREEN<<"    PASS: "<<numPass<<BOLDRED<<"    FAIL: "<<numFail<<RESET<<std::endl<<std::endl;

	if(numFail == numTasks){
		std::cout << "\n\n[ERROR]:"<<BOLDRED<<"\t\tNo running workers available! Exiting REDUCE Phase ..."<<RESET<<std::endl<<std::endl;
		return false;	
	}

	if(failed_tasks.size()){
		std::cout << "\n\n[INFO]:"<<BOLDCYAN<<"\t\tRedoing the failed tasks from REDUCE Phase!"<<RESET<<std::endl<<std::endl;
	}


	while( failed_tasks.size() ){

			if(availableWorkers>0){
				for(auto &worker: workers){
					if(worker.status_== AVAILABLE){

						WorkerData* task = new WorkerData(worker.ip_addr,&cq_);

						int id = failed_tasks.front(); 
						failed_tasks.erase(failed_tasks.begin());

						std::string path = "intermediate/folder_" + std::to_string(id);

						task->assembleRequest("reducer",id,path,"output", localSpecCopy.userID, localSpecCopy.numOutputFiles, localFileShardCopy[0]);
	
						task->asyncSendTaskToWorker();

						std::cout<<"\n[INFO]:"<<BLUE<<" Sent RPC to Worker: "<<RESET<<worker.ip_addr<<"   Reducer_ID: "<<id<<std::endl<<std::endl;

						// update the status of the worker->
						worker.status_ = BUSY;

						// Decrement the available number of workers
						--availableWorkers;

						// We found one available worker and assigned a task. so break out!
						break;
					}				
				}
			}
			else{

				void* got_tag;
        		bool ok = false;

				GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call; Waiting for someone to respond!
       		    GPR_ASSERT(ok);

				std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();

				for(auto &worker: workers){

					if(ip_addr.compare(worker.ip_addr)==0){
						
						//GPR_ASSERT(got_tag == worker->tag);

        	   		 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
							int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
							std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
						
							std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Reducer_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;

							// update the status of
							worker.status_ = AVAILABLE;

							// Since we received a reply back, 
							// increment the available number of workers.
							++availableWorkers;
						}
        	    	 	else{
							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
        					std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

							worker.status_ = FAILED;
							// Since this a failed worker, we need to 
							// redo the work for this mapper. So track this id
							// in the failed vector.
							failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());		
						}
					
						static_cast<WorkerData*>(got_tag)->destroy();

						// we found our guy! break out!
						break;
					}
				}
			}

			// Look out of the remaining mappers.
			while( availableWorkers != workers.size() - getNumFailedWorkers()){
				void* got_tag;
    		    bool ok = false;

				GPR_ASSERT(cq_.Next(&got_tag, &ok)); // This is a blocking call;
    		   	GPR_ASSERT(ok);

				std::string ip_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
				for(auto &worker: workers){

					if(ip_addr.compare(worker.ip_addr)==0){
						
						//GPR_ASSERT(got_tag == worker->tag);

    		    	 	if(static_cast<WorkerData*>(got_tag)->isStatusOk()){

							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
							int reply_id = static_cast<WorkerData*>(got_tag)->get_reply_id();
							std::string reply_status = static_cast<WorkerData*>(got_tag)->get_reply_status();
							
							std::cout<<"[INFO]:"<<GREEN<<" RPC successful!"<<RESET<<" from worker_addr: "<<GREEN<<worker_addr<<RESET<<"   Reducer_ID: "<<GREEN<<reply_id<<RESET<<"   Status: "<<BOLDGREEN<<reply_status<<RESET<<std::endl;
							
							// update the status of
							worker.status_ = AVAILABLE;

							// Since we received a reply back, 
							// increment the available number of workers.
							++availableWorkers;
						}
    		    	 	else{
							std::string worker_addr = static_cast<WorkerData*>(got_tag)->get_ip_addr();
							std::cout << "\n[INFO]:"<<BOLDRED<<" RPC failed!"<<RESET<<" from Worker_addr: "<<BOLDRED<<worker_addr<<RESET<< std::endl<<std::endl;

							worker.status_ = FAILED;

							// Since this a failed worker, we need to 
							// redo the work for this mapper. So track this id
							// in the failed vector.
							failed_tasks.push_back(static_cast<WorkerData*>(got_tag)->get_mr_id());		
						}
						

						static_cast<WorkerData*>(got_tag)->destroy();

						// we found our guy! break out!
						break;
					}
				}
			}
	}

	std::cout << "\n\n[INFO]:"<<BOLDGREEN<<" Completed all MAP and REDUCE tasks! "<<RESET<<std::endl<<std::endl;
	
	std::cout << "[INFO]:"<<BOLDCYAN<<"\t\tAll tasks done! Master exiting.. Good Bye ! "<<RESET<<std::endl<<std::endl;



	return true;

}



