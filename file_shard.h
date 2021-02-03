#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <list>
#include <map>
#include <string>
#include <fstream>
#include <iterator>

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {		
	std::map <std::string, std::pair <std::streampos,std::streampos> > shardInfo;
};

struct shardRecord{
	std::string filename;
	std::streampos start_pos;
	std::streampos end_pos;
	unsigned long size;
};


inline int processFile(std::string &filename, std::streampos &start_pos, std::streampos &end_pos, unsigned long &size, bool &metFileEnd, bool &metShardSize){

	// open th file handle to read
	std::ifstream file(filename);
	if(file.is_open()){

		// create a local begin 
		// and end variables.
		std::streampos begin = 0;
		std::streampos end = 0;	

		file.seekg(start_pos);	// set the initial position of file pointer

		begin = file.tellg(); // get the current position of file handle.

		std::string line;
		while(getline(file,line)){
			end  = file.tellg();

			if(end == EOF){
				std::ifstream file1(filename);
				file1.seekg(0,std::ios::end);
				end = file1.tellg();
				file1.close();
			}
			
			size = (end-begin);
			end_pos = end;
				
			if( size >= 1){
				// met the shard size.
				metShardSize = true;					
				break;
			}
			//std::cout<<"-->"<<line<<"<--"<<std::endl;	
		}
	
		// If we reach here, means met the file end.
		file.seekg(file.end);
		if(file.tellg() == -1){
			metFileEnd = true;
		}
		
	}else{
		std::cout<<"Cannot open the file to read!"<<std::endl;
		return -1;
	}

	// close the file.
	file.close();

	return 1;
}




/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {

	std::string filename;
	std::streampos start_pos = 0; 
	std::streampos end_pos   = 0; 
	bool metFileEnd   = false;
	bool metShardSize = false;
	unsigned long size = 0;		

	int success = -1;

	std::vector<std::string>inputfiles;
	inputfiles.assign(mr_spec.inputFiles.begin(),mr_spec.inputFiles.end());

	std::list<shardRecord> shards;

	while(true){

		if(metFileEnd==false && metShardSize==false){

			// begining state.
			if(inputfiles.size()){
			
				filename = inputfiles.front();
				inputfiles.erase(inputfiles.begin());
			}else{
				std::cout<<"Empty list!"<<std::endl;
				break;
			}

			success = processFile(filename, start_pos, end_pos, size, metFileEnd, metShardSize);

		}
		else if(metFileEnd==false && metShardSize==true){
			
			shardRecord temp;
			temp.filename = filename;
			temp.start_pos = start_pos;
			temp.end_pos = end_pos;
			temp.size = end_pos-start_pos;
			shards.push_back(temp);

			// set the state variables.
			
			size=0;
			start_pos = end_pos;
			metShardSize = false;
			metFileEnd = false;

			success = processFile(filename, start_pos, end_pos, size, metFileEnd, metShardSize);
		}
		else if(metFileEnd==true && metShardSize==false){

			// file has end.
			shardRecord temp;
			temp.filename = filename;
			temp.start_pos = start_pos;
			temp.end_pos = end_pos;
			temp.size = end_pos-start_pos;
			shards.push_back(temp);

			//set the state variables.
			if(inputfiles.size()){
				filename = inputfiles.front();
				inputfiles.erase(inputfiles.begin());
			}else{
				//std::cout<<"Empty list!"<<std::endl;
				break;
			}

			start_pos = 0;
			end_pos = 0;
			metShardSize = false;
			metFileEnd = false;

			success = processFile(filename, start_pos, end_pos, size, metFileEnd, metShardSize);
			
		}
		else{
			// both file ends as well as shard also ends.
			shardRecord temp;
			temp.filename = filename;
			temp.start_pos = start_pos;
			temp.end_pos = end_pos;
			temp.size = end_pos-start_pos;
			shards.push_back(temp);

			//set the state variables.
			if(inputfiles.size()){
				filename = inputfiles.front();
				inputfiles.erase(inputfiles.begin());
			}else{
				//std::cout<<"Empty list!"<<std::endl;
				break;
			}

			size=0;
			start_pos = 0;
			end_pos = 0;
			metShardSize = false;
			metFileEnd = false;

			success = processFile(filename, start_pos, end_pos, size, metFileEnd, metShardSize);

		}
		

	}
	


	// clean the vector of fileshards here.
	fileShards.clear();	

	std::map< std::string, std::pair <std::streampos,std::streampos> >::iterator itr;

	std::string curr_filename;	
	FileShard tempShard;

	unsigned long count=0;

	for(auto &x: shards){
		if(count == 0){
			//std::cout << "count equal to 0" << std::endl;
			tempShard.shardInfo.clear();
			tempShard.shardInfo.emplace(x.filename,make_pair(x.start_pos,x.end_pos));
			count += x.size; 
			curr_filename = x.filename;
		}else{

			if(curr_filename.compare(x.filename) == 0){
				//file name is the same as previous one
				itr = tempShard.shardInfo.find(x.filename);
				if(itr != tempShard.shardInfo.end()){
					(itr->second).second = x.end_pos;
				}
				count += x.size; 
			}
			else{
				//file name is different from previous one
				tempShard.shardInfo.emplace(x.filename,make_pair(x.start_pos,x.end_pos));
				count += x.size;
				curr_filename = x.filename;
			}

		}

		if(count >= (1024*mr_spec.shardSizeInKB)){
			count = 0;
			fileShards.push_back(tempShard);
		}
	}

	fileShards.push_back(tempShard);

	return true;
}
