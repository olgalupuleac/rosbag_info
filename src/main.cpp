#include <assert.h>
#include "getYamlInfo.h"


//note: this code only works with specified key and prints info in yaml format

using std::pair;
using std::string;
using std::unordered_set;

struct Args{
    bool y = false;
    bool freq = false;
    string key;
   unordered_set<string> files;
};

Args parse_arguments(int argc, char* argv[]) {
    Args res;
    int i = 1;
    while (i < argc) {
        if (std::string(argv[i]) == "-y")
            res.y = true;
        else
            if (std::string(argv[i]) == "-k" || std::string(argv[i]) == "--key")
                 res.key = std::string(argv[++i]);
            else
                if(std::string(argv[i]) == "--freq")
                    res.freq = true;
                else
                    res.files.insert(string(argv[i]));
        i++;
    }
    assert(res.y || res.key != "" || res.files.size());
    return res;
}

//todo use boost library for parsing arguments


int main(int argc, char* argv[]){
    Args args = parse_arguments(argc, argv);
    freopen("/home/olga/out.txt", "w", stderr);
    rosbag::printYamlInfo(args.files, args.key, args.freq);
    return 0;
}
