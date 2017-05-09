//
// Created by olga on 08.05.17.
//

#ifndef MYPAKCAGE_GETYAMLINFO_H
#define MYPAKCAGE_GETYAMLINFO_H


#include "RosbagInfo.h"

YAML::Emitter rosbag::getYamlInfo(const std::string& filename, const std::string& key);



#endif //MYPAKCAGE_GETYAMLINFO_H
