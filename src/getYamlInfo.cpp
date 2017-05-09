//
// Created by olga on 08.05.17.
//
#include <iostream>
#include "getYamlInfo.h"

using std::string;
using ros::Time;
using ros::Duration;

namespace rosbag{

    uint32_t printable_nsec(uint64_t nsec){
        nsec %= (uint32_t)1e9;
        while(nsec < (uint32_t)1e9)
            nsec *= 10;
        return nsec;
    }

    void printYamlInfo(const string filename, const string key){
        YAML::Emitter info;
        if(key == "path"){
            BagInfo bag(filename, READ_VERSION);
            info << filename;
        }
        if(key == "version"){
            BagInfo bag(filename, READ_VERSION);
            info << bag.getMajorVersion() << "." << bag.getMinorVersion();
        }
        if(key == "size"){
            BagInfo bag(filename, READ_VERSION);
            info << bag.getSize();
        }
        if(key == "start"){
            BagInfo bag(filename);
            Time time = bag.getStartTime();
            info << time.sec << "." << printable_nsec(time.nsec) ;
        }
        if(key == "end"){
            BagInfo bag(filename);
            Time time = bag.getEndTime();
            info << time.sec << "." << printable_nsec(time.nsec) ;
        }
        if(key == "duration"){
            BagInfo bag(filename);
            Duration time = bag.getEndTime() - bag.getStartTime();
            info << time.sec << "." << printable_nsec(time.nsec) ;
        }
        if(key == "indexed"){
            BagInfo bag(filename);
            info << (bag.isIndexed() ? "True" : "False");
        }
        if(key == "messages"){
            BagInfo bag(filename);
            info << bag.getMessagesNumber();
        }
        if(key == "compression"){
            BagInfo bag(filename, READ_CHUNKS);
            auto compression_info = bag.getCompression();
            info << std::get<0>(compression_info);
            if(std::get<2>(compression_info)){
                info << YAML::BeginMap;
                info << YAML::Key << "uncompressed";
                info << YAML::Value << std::get<1>(compression_info);
                info << YAML::Key << "compressed";
                info << YAML::Value << std::get<2>(compression_info);
                info << YAML::EndMap;
            }
        }
        if(key == "topics"){
            BagInfo bag(filename);
            auto topics = bag.getTopics();
            info << YAML::BeginSeq;
            for(const auto& topic : topics){
                info << YAML::BeginMap;
                info << YAML::Key << "topic";
                info << YAML::Value << topic.first.first;
                if(topic.second.second > 1){
                    info << YAML::Key << "connections";
                    info << YAML::Value << topic.second.second;
                }
                info << YAML::Key << "messages";
                info << YAML::Value << topic.second.first;
                info << YAML::Key << "type";
                info << YAML::Value << topic.first.second;
                //todo make it more readable
                info << YAML::EndMap;
            }
            info << YAML::EndSeq;
        }
        if(key == "types"){
            BagInfo bag(filename);
            auto types = bag.getTypes();
            info << YAML::BeginSeq;
            for(const auto& type : types){
                info << YAML::BeginMap;
                info << YAML::Key << "type";
                info << YAML::Value << type.first;
                info << YAML::Key << "md5";
                info << YAML::Value << type.second;
                info << YAML::EndMap;
            }
            info << YAML::EndSeq;
        }
        std::cout << info.c_str();
    }
}


