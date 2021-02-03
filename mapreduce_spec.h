#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <iterator>
#include <algorithm>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdlib.h>


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



/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	std::vector<std::string> workers;
	std::vector<std::string> inputFiles;
	unsigned int numWorkers;
	unsigned int numOutputFiles;
	std::string outDirName;	
	unsigned int shardSizeInKB;
	std::string userID;
};


/* Function to parse a line by ',' delimiter */
inline void parseStringsByComma(std::string &line, std::vector<std::string> &holder){  
	std::stringstream ss(line); 
	while (ss.good()) {
        std::string substr;
        std::getline(ss, substr, ',');
        holder.push_back(substr);
    }
}


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
		std::ifstream file(config_filename);
	if(file.is_open()){
		std::string line;

		while(std::getline(file,line)){	

			std::stringstream ss(line); 
			std::string substr;
			std::getline(ss, substr, '=');
			
			if(substr.compare("n_workers")==0){
				std::getline(ss, substr,'=');
				mr_spec.numWorkers = stoi(substr,nullptr,10);
			}

			if(substr.compare("n_output_files")==0){
				std::getline(ss, substr,'=');
				mr_spec.numOutputFiles = stoi(substr,nullptr,10);
			}

			if(substr.compare("map_kilobytes")==0){
				std::getline(ss, substr,'=');
				mr_spec.shardSizeInKB = stoi(substr,nullptr,10);
			}

			if(substr.compare("output_dir")==0){
				std::getline(ss, substr,'=');
				mr_spec.outDirName = substr;
			}

			if(substr.compare("user_id")==0){
				std::getline(ss, substr,'=');
				mr_spec.userID = substr;
			}

			if(substr.compare("worker_ipaddr_ports")==0){
				std::getline(ss, substr,'=');
				parseStringsByComma(substr, mr_spec.workers);
			}

			if(substr.compare("input_files")==0){
				std::getline(ss, substr,'=');
				parseStringsByComma(substr, mr_spec.inputFiles);
			}
		}
	}
	else{
		std::cout<<"\n[ERROR]:\tError! Cannot open the file: "<<config_filename<<" to read!"<<std::endl<<std::endl;
		return false;
	}

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	std::cout<<"\n\n\n[INFO]: Validating "<<BOLDMAGENTA<<" config.ini "<<RESET;
	// verify if all the worker ip are unique.	
	if(!(mr_spec.numWorkers > 0)){
		std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tn_workers"<<RESET<<" is not a positive integer"<<std::endl;
		return false;
	}
	if(mr_spec.numWorkers != mr_spec.workers.size()){
		std::cout<<"\n[ERROR]:\tMismatch between "<<BOLDRED<<"n_workers"<<RESET<<" and number of "<<BOLDRED<<"worker_ipaddr_ports"<<RESET<<std::endl;
		return false;
	}
	if(!(mr_spec.numOutputFiles > 0)){
		std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tn_output_files"<<RESET<<" is not a positive integer"<<std::endl;
		return false;
	}
	if(!(mr_spec.shardSizeInKB > 0)){
		std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tmap_kilobytes"<<RESET<<" is not a positive integer"<<std::endl;
		return false;
	}
	if((mr_spec.userID.compare("")==0)){
		std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tuser_id"<<RESET<<" is not defined"<<std::endl;
		return false;
	}
	if((mr_spec.outDirName.compare("")==0)){
		std::cout<<"\n[ERROR]:"<<BOLDRED<<"\toutput_dir"<<RESET<<" is not defined!"<<std::endl;
		return false;
	}

	for(auto &x: mr_spec.inputFiles){
		std::ifstream file(x);
		if(!file.is_open()){ 
			std::cout<<"\n[ERROR]:"<<BOLDRED<<"\tError! Cannot open the file: "<<RESET<<x<<std::endl;
			return false;
		}else{
			if(file.bad()){
				std::cout<<"\n[ERROR]:\tError! Improper File!: "<<x<<std::endl;
				return false;
			}
		}
	}	

	std::cout<<BOLDGREEN<<"-  all OK!\n"<<RESET<<std::endl;	
	
	return true;
}
