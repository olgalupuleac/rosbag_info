#include <assert.h>
#include "getYamlInfo.h"


//note: this code only works with specified key and prints info in yaml format

using std::pair;
using std::string;

struct Args{
    bool y = false;
    std::string key;
    std::string filename;
};

Args parse_arguments(int argc, char* argv[]) {
    assert(argc == 5);
    Args res;
    int i = 1;
    while (i < 5) {
        if (std::string(argv[i]) == "y")
            res.y = true;
        else
            if (std::string(argv[i]) == "-k" || std::string(argv[i]) == "--key")
                 res.key = std::string(argv[++i]);
            else
                res.filename = argv[i];
        i++;
    }
    assert(res.y || res.key != "" || res.filename != "");
    return res;
}

//todo use boost library for parsing arguments


int main(int argc, char* argv[]){
    Args args = parse_arguments(argc, argv);
    rosbag::printYamlInfo(args.filename, args.key);
    return 0;
}
