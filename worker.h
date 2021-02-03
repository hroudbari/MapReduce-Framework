#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

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


#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
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


/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);


enum CallStatus { CREATE, PROCESS, FINISH };



class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		void parseRequest(WorkerTask* request_);

		void actAsMapper();
		
		void actAsReducer();


	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		
		// Server data members
		std::string server_addr;

		typedef struct shardRecord{
			std::string filename;
			std::streampos start_pos;
			std::streampos end_pos;
			unsigned long size;
		}shardRecord;


		// Fields required to serve the requests;
		std::string workertype;
		int worker_id;
		int n_outfiles;
		std::string in_dirpath;
		std::string out_dirpath;
		std::string userid;

		std::vector<shardRecord> shards;


		// output messages.
		std::string status;

};

/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	this->server_addr = ip_addr_port;
	
}


class CallData{
			
	private:
		Masterworker::AsyncService* service_;
		//service = &service_;
		ServerCompletionQueue* cq_;
		//cq = cq_.get();

		ServerContext ctx_; 

		WorkerTask request_;		
		WorkerReply reply_;

		ServerAsyncResponseWriter<WorkerReply> responder_ ;

		 //enum CallStatus { CREATE, PROCESS, FINISH };
    	 CallStatus status_;


	public:

	
		CallData(Masterworker::AsyncService* service, ServerCompletionQueue* cq)
        	: service_(service), cq_(cq), responder_(&ctx_),status_(CREATE){

      		//service_->RequestSendWorkerTask(&ctx_, &request_, &responder_, cq_, cq_,this);
			Proceed();	
    	}

		
	
		WorkerTask* getRequest(){
			//new CallData(service_, cq_);
			return(&(this->request_));
		}

		WorkerReply* getReply(){
			//new CallData(service_, cq_);
			return(&(this->reply_));
		}

		
		/*
		void prepareReply(int &worker_id, std::string &status){
			// set the fields of reply message here
			this->reply_.set_id(worker_id);
			this->reply_.set_status(status);	
		}

		*/

		/*
			Please don't enable this! This will call default desctructor on the 
			already deleted call data object in the second iteration.

		~CallData(){
			delete this;
		}
		*/

		 CallStatus getStatus(){
			return(status_);
		 }


	
	void Proceed(){

		if(status_ == CREATE){
			service_->RequestSendWorkerTask(&ctx_, &request_, &responder_, cq_, cq_,this);
			status_ = PROCESS;
		}else if(status_ == PROCESS){

			new CallData(service_, cq_);
			//reply_.set_id(1);
			//reply_.set_status("true");
			responder_.Finish(reply_, Status::OK,this);
			status_ = FINISH;

		}else{
			GPR_ASSERT(status_== FINISH);
			delete this;
		}

	}
	

};




/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/

bool Worker::run() {

	std::unique_ptr<ServerCompletionQueue> cq_;
	Masterworker::AsyncService service_;
	std::unique_ptr<Server> server_;
	

	// --- Building the server --- //
	ServerBuilder builder;
	builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
	builder.RegisterService(&service_);
	cq_ = builder.AddCompletionQueue();
	server_ = builder.BuildAndStart();

	std::cout << "\n\n[INFO]:"<<BOLDBLUE<<"\tWorker listening on port  " <<RESET BOLDMAGENTA<< server_addr <<RESET<< std::endl;

	
	new CallData(&service_, cq_.get());

	void* tag;
	bool ok = false;    


	while(true){

		//std::cout<<"\nReached here 1 !"<<std::endl;	

		GPR_ASSERT(cq_->Next(&tag, &ok));
		//cq_->Next(&tag, &ok);
     
		//std::cout<<"\nReached here 2 !"<<std::endl;

		GPR_ASSERT(ok);


		if( static_cast<CallData*>(tag)->getStatus() == PROCESS ){
			
			WorkerTask* request = static_cast<CallData*>(tag)->getRequest();
			// do parsing here;
			parseRequest(request);

			if(workertype.compare("mapper")==0){
				actAsMapper();
				status="PASS";

			}else if(workertype.compare("reducer")==0){

				actAsReducer();
				status="PASS";
			}else{
				status = "FAIL";
				std::cout<<" No type Specified !"<<std::endl;
			}	

			WorkerReply* reply = static_cast<CallData*>(tag)->getReply();

			reply->set_id(worker_id);
			reply->set_status(status);

			//static_cast<CallData*>(tag)->prepareReply(worker_id, status);

		}

		static_cast<CallData*>(tag)->Proceed();
		

			

		/*
		//if(tag == (void*)1){

      	WorkerTask* request = static_cast<CallData*>(tag)->getRequest();

		// do parsing here;
		parseRequest(request);

		if(workertype.compare("mapper")==0){
			actAsMapper();

		}else if(workertype.compare("reducer")==0){

			actAsReducer();
		}else{
			status = "FAIL";
			std::cout<<" No type Specified !"<<std::endl;
		}	


		status = "true";
		
		static_cast<CallData*>(tag)->respond(worker_id,status);
		
		//static_cast<CallData*>(tag)->~CallData();
		//}
		*/

	}
	
		server_->Shutdown();
		
		// Always shutdown the completion queue after the server.
	    cq_->Shutdown();

	return true;
}

void Worker::parseRequest(WorkerTask *request_){

	workertype  = request_->workertype();
	worker_id   = request_->id();
	n_outfiles  = request_->n_outfiles();
    in_dirpath  = request_->in_dirpath();
	out_dirpath = request_->out_dirpath();
	userid		= request_->userid();

	//std::cout<<"Contents of the message: "<<std::endl;
	//std::cout<<"workertype: "<<workertype<<std::endl;
	//std::cout<<"worker_id: "<<worker_id<<std::endl;
	//std::cout<<"n_outfile: "<<n_outfiles<<std::endl;

	
	// Clear the previous file shards.
	shards.clear();

	for(auto &record : request_->shards()){
		shardRecord temp;
		temp.filename  = record.filepath();
		temp.start_pos = record.start_pos();
		temp.end_pos   = record.end_pos();
		temp.size      = record.size();

		shards.push_back(temp);
	}

}

void Worker::actAsMapper(){

	//std::cout<<"Entered here as a Mapper! "<<std::endl;

	//auto mapper = get_mapper_from_task_factory(userid); // need to check if we can access here.
	auto mapper = get_mapper_from_task_factory(userid); // need to check if we can access here.

	mapper->impl_->interDirPath = out_dirpath;
	mapper->impl_->numOutFiles  = n_outfiles;
	mapper->impl_->mapperid     = worker_id;


	// clear any previous intermediate file for this mapper_id
	for(int signature = 0; signature < n_outfiles; ++signature){
		std::string filepath = out_dirpath+"/folder_"+std::to_string(signature)+"/mapper-id_"+std::to_string(worker_id)+".txt";
		std::string cmd;

		cmd = "rm -f "+filepath;

		if(system(cmd.c_str()) == -1){
			std::cout<<"\n[ERROR]\tError! Cannot delete the file:  "<<cmd<<std::endl;
		}else{
			//std::cout<<"Deleted file:  "<<cmd<<std::endl;
		}

	}


	for(auto &record : shards){
		std::string filename     = record.filename;
		std::streampos start_pos = record.start_pos;
		std::streampos end_pos   = record.end_pos;
		

		std::ifstream file(filename);
		file.seekg(start_pos);

		std::string line;

		if(file.is_open()){
			while(std::getline(file,line)){
				mapper->map(line);
							
				if(file.tellg()==end_pos){
					break;
				}
			}
		}
		
		file.close();
	}		

}

void Worker::actAsReducer(){
	auto reducer = get_reducer_from_task_factory(userid); // need to check if we can access here.


	// clear any previous output file for this mapper_id
	std::string filepath = 	out_dirpath+"/reducer-"+std::to_string(worker_id)+"_output.txt";
	std::string cmd;

	cmd = "rm -f "+filepath;

	if(system(cmd.c_str()) == -1){
		std::cout<<"\n[ERROR]:\tError! Cannot delete the file:  "<<cmd<<std::endl;
	}else{
		//std::cout<<"Deleted file:  "<<cmd<<std::endl;
	}


	reducer->impl_->populateKeyVectorsMap(in_dirpath);

	for(auto &x: reducer->impl_->keyVectorsMap){
		reducer->reduce(x.first,x.second);
	}

	reducer->impl_->genOutputFile(out_dirpath,worker_id);
	
}
