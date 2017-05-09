//
// Created by olga on 08.05.17.
//

#include "RosbagInfo.h"

namespace rosbag{
    YAML::Emitter getYamlInfo(const std::string& filename, const std::string& key) {
        YAML::Emitter info;
        if(key == "path"){
            BagInfo bag(filename, READ_VERSION);
            info << filename;
        }
        if(key == "size"){
            BagInfo bag(filename, READ_VERSION);
            uint64_t offset = bag.file_.getOffset();
            bag.seek(0, std::ios::end);
            bag.file_size_ = bag.file_.getOffset();
            bag.seek(offset);
            info << bag.file_.getOffset();
        }
        if(key == "version"){
            BagInfo bag(filename, READ_VERSION);
            info << bag.getMajorVersion();
            info << ".";
            info << bag.getMinorVersion();
        }
        if(key == "start"){
            /*                if self._chunks:
                        start_stamp = self._chunks[ 0].start_time.to_sec()
                        end_stamp   = self._chunks[-1].end_time.to_sec()
                    else:
                        start_stamp = min([index[ 0].time.to_sec() for index in self._connection_indexes.values()])
                        end_stamp   = max([index[-1].time.to_sec() for index in self._connection_indexes.values()])*/
            rosbag::Bag bag(filename, READ_CHUNK_INFO);
            ros::Time start_stamp = ros::TIME_MAX;
            if(bag.chunks_.size())
                ros::Time start_stamp = bag.chunks_[0].start_time;
                //ros::Time end_time = bag.chunks_.back().end_time;
            else
                for(const auto& index : bag.connection_indexes_)
                    if(index.second.time < start_stamp)
                        start_stamp = index.second.time;
            info << start_stamp;
        }
        /* if(key == "end"){
             os << end_time_ << "\n";
         }
         if(key == "duration"){
             os << end_time_ - start_time_ << "\n";
         }
         if(key == "compression") {
             uint32_t main_compression_count = 0;
             std::string main_compression_type;
             uint32_t compressed = 0;
             uint32_t uncompressed = 0;
             for(const auto& compression_type : compression_type_count_){
                 if(compression_type.second > main_compression_count){
                     main_compression_count = compression_type.second;
                     main_compression_type = compression_type.first;
                 }
                 if(compression_type.first == COMPRESSION_NONE)
                     uncompressed++;
                 else
                     compressed++;
             }
             os << main_compression_type << "\n";
             if(compressed){
                 os << "uncompressed: " << uncompressed << "\n" <<
                    "compressed: " << compressed << "\n";
             }
         }
         if(key == "indexed"){
             if(chunks_.size() || connection_indexes_.size())
                 os << "True";
             else
                 os << "False";
             os << "\n";
         }
         if(key == "messages"){
             /*              if self._chunks:
                     num_messages = 0
                     for c in self._chunks:
                         for counts in c.connection_counts.values():
                             num_messages += counts
                 else:
                     num_messages = sum([len(index) for index in self._connection_indexes.values()])*/
        /* uint64_t num_msg = 0;
         if(chunks_.size())
             for(const auto& chunk : chunks_)
                 for(const auto& count : chunk.connection_counts)
                     num_msg += count.second;
         else
             for(const auto& index : connection_indexes_)
                 num_msg += index.second.size();
         os << num_msg << "\n";
     }
     if(key == "topics") {
         os << "\n";
         for (const auto &connection : connections_) {
             ConnectionInfo *connection_info = connection.second;
             os << "-topic: " << connection_info->topic << "\n";
             os << "type: " << connection_info->datatype << "\n";
             uint64_t msg_count = 0;
             for (const auto &chunk : chunks_) {
                 auto it = chunk.connection_counts.find(connection_info->id);
                 if (it != chunk.connection_counts.end())
                     msg_count += it->second;
             }
             os << "messages: " << msg_count << "\n";
         }
     }
     if(key == "types"){
         os << "\n";
         for(const auto& connection : connections_){
             ConnectionInfo* connection_info = connection.second;
             os << "-type: " << connection_info->datatype << "\n";
             os << "md5: " << connection_info->md5sum << "\n";
         }
     }*/

    }


}


