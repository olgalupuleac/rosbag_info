//
// Created by olga on 08.05.17.
//

#ifndef MYPAKCAGE_GETYAMLINFO_H
#define MYPAKCAGE_GETYAMLINFO_H


#include "RosbagInfo.h"
#include "yaml-cpp/yaml.h"

namespace rosbag{
    void printYamlInfo(const std::string filename, const std::string key);
}





#endif //MYPAKCAGE_GETYAMLINFO_H
