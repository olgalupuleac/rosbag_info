#include <iostream>
#include "ctime"
#include "getYamlInfo.h"

void get_time_difference(clock_t& begin){
    std::cout << "\n" << float(clock() - begin) /CLOCKS_PER_SEC << "\n";
    begin = clock();
}


int main(){
    clock_t begin_time = clock();
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "indexed");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "start");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "end");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "duration");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "path");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "messages");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "version");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "size");
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "topics");
    get_time_difference(begin_time);
    return 0;
}
