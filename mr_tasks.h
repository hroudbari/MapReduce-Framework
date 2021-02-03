#pragma once

#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <iterator>
#include <fstream>
#include <vector>
#include <dirent.h>

#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		// directory path for intermediate files to be set by the worker::actAsMapper();
		std::string interDirPath;

		// Number of out files.
		int numOutFiles;

		// Mapper-id to be set by the worker::actAsMapper();
		int mapperid;

};



/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;

	// In this function we need to increment the value for the emitted key in the map.
	std::string signature = std::to_string(std::hash<std::string>{}(key)%numOutFiles);
	std::string path = interDirPath+"/folder_"+signature+"/mapper-id_"+std::to_string(mapperid)+".txt";
	std::ofstream outfile;
	outfile.open(path,std::ios_base::app); // append instead of overwrite
	outfile <<key<<" "<<val<<std::endl;
	outfile.close();

}

/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		// Need a structure to store the files.
		std::vector<std::string> inputfiles;
		inline bool collectIntermediateFiles(const std::string &path); 
		
		
		std::map< std::string, std::vector<std::string> >keyVectorsMap;
		inline bool populateKeyVectorsMap(const std::string &path);		

		
		std::map< std::string, std::string> finalMap;
		inline void genOutputFile(const std::string &outDirPath, int reducerid);

};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}

inline void BaseReducerInternal::genOutputFile(const std::string &outDirPath, int reducerid){

	std::string temp_filename = outDirPath+"/temp_reducer-"+std::to_string(reducerid)+"_output.txt";
	std::string filename = 	outDirPath+"/reducer-"+std::to_string(reducerid)+"_output.txt";

	std::ofstream outfile;
	outfile.open(temp_filename);
	for (auto&x: finalMap){
		outfile << x.first << " " << x.second << std::endl;
	}
	outfile.close();


	
	std::string cmd;

	cmd = "sort -d "+temp_filename+" > "+filename;
	if(system(cmd.c_str()) == -1){
		std::cout<<"\n[ERROR]:\tError! Cannot sort the file:  "<<cmd<<std::endl;
	}else{
		//std::cout<<"sorted the file :  "<<cmd<<std::endl;
	}

	cmd = "rm -f "+temp_filename;
	if(system(cmd.c_str()) == -1){
		std::cout<<"\n[ERROR]:\tError! Cannot delete the file:  "<<cmd<<std::endl;
	}else{
		//std::cout<<"Deleted file:  "<<cmd<<std::endl;
	}

	
	this->finalMap.clear();
	this->keyVectorsMap.clear();
	this->inputfiles.clear();
}


inline bool BaseReducerInternal::populateKeyVectorsMap(const std::string &path){

	// step-1 : grab all the intermediate files in the given path.
	if( collectIntermediateFiles(path) ){	
		for(auto &x: inputfiles){
			std::ifstream file(x);
			if(file.is_open()){
				std::string line;
				while(getline(file,line)){
					std::stringstream ss(line);
					std::vector<std::string> temp;
					while(ss.good()){
        				std::string substr;
        				std::getline(ss, substr,' ');
        				temp.push_back(substr);
					}
					// time to push the values into the map.
					std::map< std::string, std::vector<std::string> >::iterator itr;
					itr= keyVectorsMap.find(temp[0]);
					if(itr!=keyVectorsMap.end()){
						(itr->second).push_back(temp[1]);
					}
					else{
						std::vector<std::string> values;
						values.push_back(temp[1]);
						keyVectorsMap.emplace(temp[0],values);
					}
				}

			}
		
			file.close();
		}			
	
	}else{
		std::cout<<"\n[ERROR]:\tCannot grab any files in the path: "<<path<<std::endl;
		return false;
	}

	return true;
}

inline bool BaseReducerInternal::collectIntermediateFiles(const std::string &path){
	
	struct dirent *entry;
   	DIR *dir = opendir(path.c_str());
   
   	if (dir == NULL) {
      return false;
   	}
   	while ((entry = readdir(dir)) != NULL) {		
   		//std::cout << entry->d_name << std::endl;
		if((entry->d_type)==DT_REG){
			std::string filepath = path+"/"+entry->d_name;
			inputfiles.push_back(filepath);
		}
   	}
   	closedir(dir);

	return true;
}



/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
	
	// In this function we need to store the values emitted by emit 
	// into the finalMap.

	std::map< std::string, std::string>::iterator itr;
	itr = finalMap.find(key);

	if(itr!=finalMap.end()){
		std::cout<<"\n[ERROR]:\tError! Found a duplicate in the list! Key: "<<key<<"Value: "<<val<<std::endl;
	}else{
		finalMap.emplace(key,val);
	}
}
