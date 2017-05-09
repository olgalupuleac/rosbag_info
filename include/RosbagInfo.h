/*********************************************************************
* Software License Agreement (BSD License)
*
*  Copyright (c) 2008, Willow Garage, Inc.
*  All rights reserved.
*
*  Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions
*  are met:
*
*   * Redistributions of source code must retain the above copyright
*     notice, this list of conditions and the following disclaimer.
*   * Redistributions in binary form must reproduce the above
*     copyright notice, this list of conditions and the following
*     disclaimer in the documentation and/or other materials provided
*     with the distribution.
*   * Neither the name of Willow Garage, Inc. nor the names of its
*     contributors may be used to endorse or promote products derived
*     from this software without specific prior written permission.
*
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
*  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
*  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
*  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
*  COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
*  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
*  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
*  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
*  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
*  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
*  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
*  POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/

#ifndef ROSBAG_BAGINFO_H
#define ROSBAG_BAGINFO_H

#include "rosbag/macros.h"

#include "rosbag/buffer.h"
#include "rosbag/chunked_file.h"
#include "rosbag/constants.h"
#include "rosbag/exceptions.h"
#include "rosbag/structures.h"

#include "ros/header.h"
#include "ros/time.h"
#include "ros/message_traits.h"
#include "ros/message_event.h"
#include "ros/serialization.h"

#include "yaml-cpp/yaml.h"



//#include "ros/subscription_callback_helper.h"

#include <ios>
#include <ostream>
#include <map>
#include <unordered_map>
#include <queue>
#include <set>
#include <stdexcept>
#include <unordered_set>

#include <boost/format.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include "console_bridge/console.h"
#include "getYamlInfo.h"



namespace rosbag {
    enum ReadingMode {
        READ_CHUNK_INFO,
        READ_CHUNKS,
        READ_VERSION
    };


    struct ChunkInfoComparator{
        bool operator()(const ChunkInfo& lhs,
                        const ChunkInfo& rhs){
            return lhs.pos < rhs.pos;
        }
    };



class MessageInstance;
class View;
class Query;


class  BagInfo
{
    friend class MessageInstance;
    friend class View;
    friend YAML::Emitter getYamlInfo(const std::string& filename, const std::string& key);
public:

    explicit BagInfo(std::string const& filename, ReadingMode mode = READ_CHUNK_INFO);

    ~BagInfo();


    //! Close the bag file
    void close();

    std::string     getFileName()     const;                      //!< Get the filename of the bag
    uint32_t        getMajorVersion() const;                      //!< Get the major-version of the open bag file
    uint32_t        getMinorVersion() const;                      //!< Get the minor-version of the open bag file

    void            setCompression(CompressionType compression);  //!< Set the compression method to use for writing chunks
    CompressionType getCompression() const;                       //!< Get the compression method to use for writing chunks
    void            setChunkThreshold(uint32_t chunk_threshold);  //!< Set the threshold for creating new chunks
    uint32_t        getChunkThreshold() const;                    //!< Get the threshold for creating new chunks
    std::string getInfo(const std::string& key);



private:
    // This helper function actually does the write with an arbitrary serializable message

    void openRead  (std::string const& filename);


    template<class T>
    boost::shared_ptr<T> instantiateBuffer(IndexEntry const& index_entry) const;  //!< deserializes the message held in record_buffer_



    void startReadingVersion102();
    void startReadingVersion200();

    // Reading

    void readVersion();
    void readFileHeaderRecord();
    void readConnectionRecord();
    void readChunkHeader(ChunkHeader& chunk_header) const;
    void readChunkInfoRecord();
    void readConnectionIndexRecord200();

    void readTopicIndexRecord102();
    void readMessageDefinitionRecord102();
    void readMessageDataRecord102(uint64_t offset, ros::Header& header) const;

    ros::Header readMessageDataHeader(IndexEntry const& index_entry);
    uint32_t    readMessageDataSize(IndexEntry const& index_entry) const;

    template<typename Stream>
    void readMessageDataIntoStream(IndexEntry const& index_entry, Stream& stream) const;

    void     decompressChunk(uint64_t chunk_pos) const;
    void     decompressRawChunk(ChunkHeader const& chunk_header) const;
    void     decompressBz2Chunk(ChunkHeader const& chunk_header) const;
    void     decompressLz4Chunk(ChunkHeader const& chunk_header) const;
    uint32_t getChunkOffset() const;

    // Record header I/O


    void readHeaderFromBuffer(Buffer& buffer, uint32_t offset, ros::Header& header, uint32_t& data_size, uint32_t& bytes_read) const;
    void readMessageDataHeaderFromBuffer(Buffer& buffer, uint32_t offset, ros::Header& header, uint32_t& data_size, uint32_t& bytes_read) const;
    bool readHeader(ros::Header& header) const;
    bool readDataLength(uint32_t& data_size) const;
    bool isOp(ros::M_string& fields, uint8_t reqOp) const;

    // Header fields

    template<typename T>
    std::string toHeaderString(T const* field) const;

    std::string toHeaderString(ros::Time const* field) const;

    template<typename T>
    bool readField(ros::M_string const& fields, std::string const& field_name, bool required, T* data) const;

    bool readField(ros::M_string const& fields, std::string const& field_name, unsigned int min_len, unsigned int max_len, bool required, std::string& data) const;
    bool readField(ros::M_string const& fields, std::string const& field_name, bool required, std::string& data) const;

    bool readField(ros::M_string const& fields, std::string const& field_name, bool required, ros::Time& data) const;

    ros::M_string::const_iterator checkField(ros::M_string const& fields, std::string const& field,
                                             unsigned int min_len, unsigned int max_len, bool required) const;

    // Low-level I/O

    void read(char* b, std::streamsize n) const;
    void seek(uint64_t pos, int origin = std::ios_base::beg) const;

private:
    mutable ChunkedFile file_;
    int                 version_;
    CompressionType     compression_;
    uint32_t            chunk_threshold_;
    uint32_t            bag_revision_;

    uint64_t file_size_;
    uint64_t file_header_pos_;
    uint64_t index_data_pos_;
    uint32_t connection_count_;
    uint32_t chunk_count_;
    ReadingMode mode_;


    mutable std::map<std::string, uint32_t> compression_type_count_;
    std::unordered_map<uint32_t, uint32_t> msg_count_;

    // Current chunk
    bool      chunk_open_;
    ChunkInfo curr_chunk_info_;
    uint64_t  curr_chunk_data_pos_;

    std::map<std::string, uint32_t>                topic_connection_ids_;
    //std::map<std::string, std::unordered_set<uint32_t> >  my_topic_connection_ids_;
    std::map<ros::M_string, uint32_t>              header_connection_ids_;
    std::map<uint32_t, ConnectionInfo*>            connections_;

    std::vector<ChunkInfo>                         chunks_;

    std::map<uint32_t, std::multiset<IndexEntry> > connection_indexes_;
    std::map<uint32_t, std::multiset<IndexEntry> > curr_chunk_connection_indexes_;

    //ros::Time start_time_;
    //ros::Time end_time_;

    mutable Buffer   header_buffer_;           //!< reusable buffer in which to assemble the record header before writing to file
    mutable Buffer   record_buffer_;           //!< reusable buffer in which to assemble the record data before writing to file

    mutable Buffer   chunk_buffer_;            //!< reusable buffer to read chunk into
    mutable Buffer   decompress_buffer_;       //!< reusable buffer to decompress chunks into

    mutable Buffer   outgoing_chunk_buffer_;   //!< reusable buffer to read chunk into

    mutable Buffer*  current_buffer_;

    mutable uint64_t decompressed_chunk_;      //!< position of decompressed chunk
};

} // namespace rosbag

#include "rosbag/message_instance.h"

namespace rosbag {

// Templated method definitions


template<typename T>
std::string BagInfo::toHeaderString(T const* field) const {
    return std::string((char*) field, sizeof(T));
}

template<typename T>
bool BagInfo::readField(ros::M_string const& fields, std::string const& field_name, bool required, T* data) const {
    ros::M_string::const_iterator i = checkField(fields, field_name, sizeof(T), sizeof(T), required);
    if (i == fields.end())
    	return false;
    memcpy(data, i->second.data(), sizeof(T));
    return true;
}

template<typename Stream>
void BagInfo::readMessageDataIntoStream(IndexEntry const& index_entry, Stream& stream) const {
    ros::Header header;
    uint32_t data_size;
    uint32_t bytes_read;
    switch (version_)
    {
    case 200:
    {
        decompressChunk(index_entry.chunk_pos);
        readMessageDataHeaderFromBuffer(*current_buffer_, index_entry.offset, header, data_size, bytes_read);
        if (data_size > 0)
            memcpy(stream.advance(data_size), current_buffer_->getData() + index_entry.offset + bytes_read, data_size);
        break;
    }
    case 102:
    {
        readMessageDataRecord102(index_entry.chunk_pos, header);
        data_size = record_buffer_.getSize();
        if (data_size > 0)
            memcpy(stream.advance(data_size), record_buffer_.getData(), data_size);
        break;
    }
    default:
        throw BagFormatException((boost::format("Unhandled version: %1%") % version_).str());
    }
}

template<class T>
boost::shared_ptr<T> BagInfo::instantiateBuffer(IndexEntry const& index_entry) const {
    switch (version_)
    {
    case 200:
	{
        decompressChunk(index_entry.chunk_pos);

        // Read the message header
        ros::Header header;
        uint32_t data_size;
        uint32_t bytes_read;
        readMessageDataHeaderFromBuffer(*current_buffer_, index_entry.offset, header, data_size, bytes_read);

        // Read the connection id from the header
        uint32_t connection_id;
        readField(*header.getValues(), CONNECTION_FIELD_NAME, true, &connection_id);

        std::map<uint32_t, ConnectionInfo*>::const_iterator connection_iter = connections_.find(connection_id);
        if (connection_iter == connections_.end())
            throw BagFormatException((boost::format("Unknown connection ID: %1%") % connection_id).str());
        ConnectionInfo* connection_info = connection_iter->second;

        boost::shared_ptr<T> p = boost::make_shared<T>();

        ros::serialization::PreDeserializeParams<T> predes_params;
        predes_params.message = p;
        predes_params.connection_header = connection_info->header;
        ros::serialization::PreDeserialize<T>::notify(predes_params);

        // Deserialize the message
        ros::serialization::IStream s(current_buffer_->getData() + index_entry.offset + bytes_read, data_size);
        ros::serialization::deserialize(s, *p);

        return p;
	}
    case 102:
	{
        // Read the message record
        ros::Header header;
        readMessageDataRecord102(index_entry.chunk_pos, header);

        ros::M_string& fields = *header.getValues();

        // Read the connection id from the header
        std::string topic, latching("0"), callerid;
        readField(fields, TOPIC_FIELD_NAME,    true,  topic);
        readField(fields, LATCHING_FIELD_NAME, false, latching);
        readField(fields, CALLERID_FIELD_NAME, false, callerid);

        std::map<std::string, uint32_t>::const_iterator topic_conn_id_iter = topic_connection_ids_.find(topic);
        if (topic_conn_id_iter == topic_connection_ids_.end())
            throw BagFormatException((boost::format("Unknown topic: %1%") % topic).str());
        uint32_t connection_id = topic_conn_id_iter->second;

        std::map<uint32_t, ConnectionInfo*>::const_iterator connection_iter = connections_.find(connection_id);
        if (connection_iter == connections_.end())
            throw BagFormatException((boost::format("Unknown connection ID: %1%") % connection_id).str());
        ConnectionInfo* connection_info = connection_iter->second;

        boost::shared_ptr<T> p = boost::make_shared<T>();

        // Create a new connection header, updated with the latching and callerid values
        boost::shared_ptr<ros::M_string> message_header(boost::make_shared<ros::M_string>());
        for (ros::M_string::const_iterator i = connection_info->header->begin(); i != connection_info->header->end(); i++)
            (*message_header)[i->first] = i->second;
        (*message_header)["latching"] = latching;
        (*message_header)["callerid"] = callerid;

        ros::serialization::PreDeserializeParams<T> predes_params;
        predes_params.message = p;
        predes_params.connection_header = message_header;
        ros::serialization::PreDeserialize<T>::notify(predes_params);

        // Deserialize the message
        ros::serialization::IStream s(record_buffer_.getData(), record_buffer_.getSize());
        ros::serialization::deserialize(s, *p);

        return p;
	}
    default:
        throw BagFormatException((boost::format("Unhandled version: %1%") % version_).str());
    }
}


} // namespace rosbag

#endif
