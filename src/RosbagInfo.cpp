// Copyright (c) 2009, Willow Garage, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Willow Garage, Inc. nor the names of its
//       contributors may be used to endorse or promote products derived from
//       this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#include "RosbagInfo.h"
#include "rosbag/message_instance.h"
#include "rosbag/query.h"
#include "rosbag/view.h"

#if defined(_MSC_VER)
#include <stdint.h> // only on v2010 and later -> is this enough for msvc and linux?
#else
#include <inttypes.h>
#endif
#include <signal.h>
#include <assert.h>
#include <iomanip>

#include <boost/foreach.hpp>

#include "console_bridge/console.h"

#define foreach BOOST_FOREACH


using std::map;
using std::priority_queue;
using std::string;
using std::vector;
using std::multiset;
using boost::format;
using boost::shared_ptr;
using ros::M_string;
using ros::Time;

namespace rosbag {

    Bag::Bag() :
            mode_(bagmode::Write),
            version_(0),
            compression_(compression::Uncompressed),
            chunk_threshold_(768 * 1024),  // 768KB chunks
            bag_revision_(0),
            file_size_(0),
            file_header_pos_(0),
            index_data_pos_(0),
            connection_count_(0),
            chunk_count_(0),
            chunk_open_(false),
            curr_chunk_data_pos_(0),
            current_buffer_(0),
            decompressed_chunk_(0),
            start_time_(0),
            end_time_(0),
            read_all_file_(0)
    {
    }

    Bag::Bag(string const& filename, uint32_t mode, bool read_all) :
            compression_(compression::Uncompressed),
            chunk_threshold_(768 * 1024),  // 768KB chunks
            bag_revision_(0),
            file_size_(0),
            file_header_pos_(0),
            index_data_pos_(0),
            connection_count_(0),
            chunk_count_(0),
            chunk_open_(false),
            curr_chunk_data_pos_(0),
            current_buffer_(0),
            decompressed_chunk_(0),
            start_time_(ros::TIME_MAX),
            end_time_(ros::TIME_MIN),
            read_all_file_(read_all)
    {
        open(filename, mode);
    }

    Bag::~Bag() {
        close();
    }

    void Bag::open(string const& filename, uint32_t mode) {
        mode_ = (BagMode) mode;

        if (mode_ & bagmode::Read)
            openRead(filename);
        else
            throw BagException((format("Unknown mode: %1%") % (int) mode).str());

        // Determine file size
        uint64_t offset = file_.getOffset();
        seek(0, std::ios::end);
        file_size_ = file_.getOffset();
        seek(offset);
    }

    void Bag::openRead(string const& filename) {
        file_.openRead(filename);

        readVersion();
        if(read_all_file_){
            switch (version_) {
                case 102: startReadingVersion102(); break;
                case 200: startReadingVersion200(); break;
                default:
                    throw BagException((format("Unsupported bag file version: %1%.%2%") % getMajorVersion() % getMinorVersion()).str());
            }
        }
    }


    void Bag::close() {
        if (!file_.isOpen())
            return;


        file_.close();

        topic_connection_ids_.clear();
        header_connection_ids_.clear();
        for (map<uint32_t, ConnectionInfo*>::iterator i = connections_.begin(); i != connections_.end(); i++)
            delete i->second;
        connections_.clear();
        chunks_.clear();
        connection_indexes_.clear();
        curr_chunk_connection_indexes_.clear();
    }


    string   Bag::getFileName() const { return file_.getFileName(); }
    BagMode  Bag::getMode()     const { return mode_;               }
    uint64_t Bag::getSize()     const { return file_size_;          }

    uint32_t Bag::getChunkThreshold() const { return chunk_threshold_; }



    CompressionType Bag::getCompression() const { return compression_; }



// Version


    void Bag::readVersion() {
        string version_line = file_.getline();

        file_header_pos_ = file_.getOffset();

        char logtypename[100];
        int version_major, version_minor;
#if defined(_MSC_VER)
        if (sscanf_s(version_line.c_str(), "#ROS%s V%d.%d", logtypename, sizeof(logtypename), &version_major, &version_minor) != 3)
#else
        if (sscanf(version_line.c_str(), "#ROS%s V%d.%d", logtypename, &version_major, &version_minor) != 3)
#endif
            throw BagIOException("Error reading version line");

        version_ = version_major * 100 + version_minor;

        logDebug("Read VERSION: version=%d", version_);
    }

    uint32_t Bag::getMajorVersion() const { return version_ / 100; }
    uint32_t Bag::getMinorVersion() const { return version_ % 100; }

    void Bag::printInfo(std::ostream& os, const std::string& key) {
        if(key == "size"){
            os << getSize() << "\n";
        }
        if(key == "version"){
            //readVersion();
            os << getMajorVersion() << "." <<getMinorVersion() << "\n";
        }
        if(key == "start"){
            os << start_time_ << "\n";
        }
        if(key == "end"){
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
            /*                if self._chunks:
                    num_messages = 0
                    for c in self._chunks:
                        for counts in c.connection_counts.values():
                            num_messages += counts
                else:
                    num_messages = sum([len(index) for index in self._connection_indexes.values()])*/
            uint64_t num_msg = 0;
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
        }

/*ConnectionInfo* connection_info = new ConnectionInfo();
            connection_info->id       = connection_id;
            connection_info->topic    = topic;
            connections_[connection_id] = connection_info;

            topic_connection_ids_[topic] = connection_id;
        }
        else
            connection_id = topic_conn_id_iter->second;

        multiset<IndexEntry>& connection_index = connection_indexes_[connection_id];*/





    }

//

    void Bag::startReadingVersion200() {
        // Read the file header record, which points to the end of the chunks
        readFileHeaderRecord();

        // Seek to the end of the chunks
        seek(index_data_pos_);

        // Read the connection records (one for each connection)
        for (uint32_t i = 0; i < connection_count_; i++)
            readConnectionRecord();

        // Read the chunk info records
        for (uint32_t i = 0; i < chunk_count_; i++)
            readChunkInfoRecord();

        // Read the connection indexes for each chunk
                foreach(ChunkInfo const& chunk_info, chunks_) {
                        curr_chunk_info_ = chunk_info;

                        seek(curr_chunk_info_.pos);

                        // Skip over the chunk data
                        ChunkHeader chunk_header;
                        readChunkHeader(chunk_header);
                        seek(chunk_header.compressed_size, std::ios::cur);

                        // Read the index records after the chunk
                        for (unsigned int i = 0; i < chunk_info.connection_counts.size(); i++)
                            readConnectionIndexRecord200();
                    }

        // At this point we don't have a curr_chunk_info anymore so we reset it
        curr_chunk_info_ = ChunkInfo();
    }

    void Bag::startReadingVersion102() {
        try
        {
            // Read the file header record, which points to the start of the topic indexes
            readFileHeaderRecord();
        }
        catch (BagFormatException ex) {
            throw BagUnindexedException();
        }

        // Get the length of the file
        seek(0, std::ios::end);
        uint64_t filelength = file_.getOffset();

        // Seek to the beginning of the topic index records
        seek(index_data_pos_);

        // Read the topic index records, which point to the offsets of each message in the file
        while (file_.getOffset() < filelength)
            readTopicIndexRecord102();

        // Read the message definition records (which are the first entry in the topic indexes)
        for (map<uint32_t, multiset<IndexEntry> >::const_iterator i = connection_indexes_.begin(); i != connection_indexes_.end(); i++) {
            multiset<IndexEntry> const& index       = i->second;
            IndexEntry const&           first_entry = *index.begin();

            logDebug("Reading message definition for connection %d at %llu", i->first, (unsigned long long) first_entry.chunk_pos);

            seek(first_entry.chunk_pos);

            readMessageDefinitionRecord102();
        }
    }

// File header record


    void Bag::readFileHeaderRecord() {
        ros::Header header;
        uint32_t data_size;
        if (!readHeader(header) || !readDataLength(data_size))
            throw BagFormatException("Error reading FILE_HEADER record");

        M_string& fields = *header.getValues();

        if (!isOp(fields, OP_FILE_HEADER))
            throw BagFormatException("Expected FILE_HEADER op not found");

        // Read index position
        readField(fields, INDEX_POS_FIELD_NAME, true, (uint64_t*) &index_data_pos_);

        if (index_data_pos_ == 0)
            throw BagUnindexedException();

        // Read topic and chunks count
        if (version_ >= 200) {
            readField(fields, CONNECTION_COUNT_FIELD_NAME, true, &connection_count_);
            readField(fields, CHUNK_COUNT_FIELD_NAME,      true, &chunk_count_);
        }

        logDebug("Read FILE_HEADER: index_pos=%llu connection_count=%d chunk_count=%d",
                 (unsigned long long) index_data_pos_, connection_count_, chunk_count_);

        // Skip the data section (just padding)
        seek(data_size, std::ios::cur);
    }

    uint32_t Bag::getChunkOffset() const {
        if (compression_ == compression::Uncompressed)
            return file_.getOffset() - curr_chunk_data_pos_;
        else
            return file_.getCompressedBytesIn();
    }


    void Bag::readChunkHeader(ChunkHeader& chunk_header) const {
        ros::Header header;
        if (!readHeader(header) || !readDataLength(chunk_header.compressed_size))
            throw BagFormatException("Error reading CHUNK record");

        M_string& fields = *header.getValues();

        if (!isOp(fields, OP_CHUNK))
            throw BagFormatException("Expected CHUNK op not found");

        readField(fields, COMPRESSION_FIELD_NAME, true, chunk_header.compression);
        readField(fields, SIZE_FIELD_NAME,        true, &chunk_header.uncompressed_size);

        compression_type_count_[chunk_header.compression]++;


        logDebug("Read CHUNK: compression=%s size=%d uncompressed=%d (%f)", chunk_header.compression.c_str(), chunk_header.compressed_size, chunk_header.uncompressed_size, 100 * ((double) chunk_header.compressed_size) / chunk_header.uncompressed_size);
    }

// Index records


    void Bag::readTopicIndexRecord102() {
        ros::Header header;
        uint32_t data_size;
        if (!readHeader(header) || !readDataLength(data_size))
            throw BagFormatException("Error reading INDEX_DATA header");
        M_string& fields = *header.getValues();

        if (!isOp(fields, OP_INDEX_DATA))
            throw BagFormatException("Expected INDEX_DATA record");

        uint32_t index_version;
        string topic;
        uint32_t count = 0;
        readField(fields, VER_FIELD_NAME,   true, &index_version);
        readField(fields, TOPIC_FIELD_NAME, true, topic);
        readField(fields, COUNT_FIELD_NAME, true, &count);

        logDebug("Read INDEX_DATA: ver=%d topic=%s count=%d", index_version, topic.c_str(), count);

        if (index_version != 0)
            throw BagFormatException((format("Unsupported INDEX_DATA version: %1%") % index_version).str());

        uint32_t connection_id;
        map<string, uint32_t>::const_iterator topic_conn_id_iter = topic_connection_ids_.find(topic);
        if (topic_conn_id_iter == topic_connection_ids_.end()) {
            connection_id = connections_.size();

            logDebug("Creating connection: id=%d topic=%s", connection_id, topic.c_str());
            ConnectionInfo* connection_info = new ConnectionInfo();
            connection_info->id       = connection_id;
            connection_info->topic    = topic;
            connections_[connection_id] = connection_info;

            topic_connection_ids_[topic] = connection_id;
        }
        else
            connection_id = topic_conn_id_iter->second;

        multiset<IndexEntry>& connection_index = connection_indexes_[connection_id];


        for (uint32_t i = 0; i < count; i++) {
            IndexEntry index_entry;
            uint32_t sec;
            uint32_t nsec;
            read((char*) &sec,                   4);
            read((char*) &nsec,                  4);
            read((char*) &index_entry.chunk_pos, 8);   //<! store position of the message in the chunk_pos field as it's 64 bits
            Time time(sec, nsec);
            index_entry.time = time;

            //updating start and end time if necessary

            if(time < start_time_)
                start_time_ = time;
            if(time > end_time_)
                end_time_ = time;

            index_entry.offset = 0;

            logDebug("  - %d.%d: %llu", sec, nsec, (unsigned long long) index_entry.chunk_pos);

            if (index_entry.time < ros::TIME_MIN || index_entry.time > ros::TIME_MAX)
            {
                logError("Index entry for topic %s contains invalid time.", topic.c_str());
            } else
            {
                connection_index.insert(connection_index.end(), index_entry);
            }
        }
    }

    void Bag::readConnectionIndexRecord200() {
        ros::Header header;
        uint32_t data_size;
        if (!readHeader(header) || !readDataLength(data_size))
            throw BagFormatException("Error reading INDEX_DATA header");
        M_string& fields = *header.getValues();

        if (!isOp(fields, OP_INDEX_DATA))
            throw BagFormatException("Expected INDEX_DATA record");

        uint32_t index_version;
        uint32_t connection_id;
        uint32_t count = 0;
        readField(fields, VER_FIELD_NAME,        true, &index_version);
        readField(fields, CONNECTION_FIELD_NAME, true, &connection_id);
        readField(fields, COUNT_FIELD_NAME,      true, &count);

        logDebug("Read INDEX_DATA: ver=%d connection=%d count=%d", index_version, connection_id, count);

        if (index_version != 1)
            throw BagFormatException((format("Unsupported INDEX_DATA version: %1%") % index_version).str());

        uint64_t chunk_pos = curr_chunk_info_.pos;

        multiset<IndexEntry>& connection_index = connection_indexes_[connection_id];

        for (uint32_t i = 0; i < count; i++) {
            IndexEntry index_entry;
            index_entry.chunk_pos = chunk_pos;
            uint32_t sec;
            uint32_t nsec;
            read((char*) &sec,                4);
            read((char*) &nsec,               4);
            read((char*) &index_entry.offset, 4);
            index_entry.time = Time(sec, nsec);

            logDebug("  - %d.%d: %llu+%d", sec, nsec, (unsigned long long) index_entry.chunk_pos, index_entry.offset);

            if (index_entry.time < ros::TIME_MIN || index_entry.time > ros::TIME_MAX)
            {
                logError("Index entry for topic %s contains invalid time.  This message will not be loaded.", connections_[connection_id]->topic.c_str());
            } else
            {
                connection_index.insert(connection_index.end(), index_entry);
            }
        }
    }

// Connection records


    void Bag::readConnectionRecord() {
        ros::Header header;
        if (!readHeader(header))
            throw BagFormatException("Error reading CONNECTION header");
        M_string& fields = *header.getValues();

        if (!isOp(fields, OP_CONNECTION))
            throw BagFormatException("Expected CONNECTION op not found");

        uint32_t id;
        readField(fields, CONNECTION_FIELD_NAME, true, &id);
        string topic;
        readField(fields, TOPIC_FIELD_NAME,      true, topic);

        ros::Header connection_header;
        if (!readHeader(connection_header))
            throw BagFormatException("Error reading connection header");

        // If this is a new connection, update connections
        map<uint32_t, ConnectionInfo*>::iterator key = connections_.find(id);
        if (key == connections_.end()) {
            ConnectionInfo* connection_info = new ConnectionInfo();
            connection_info->id       = id;
            connection_info->topic    = topic;
            connection_info->header = boost::make_shared<M_string>();
            for (M_string::const_iterator i = connection_header.getValues()->begin(); i != connection_header.getValues()->end(); i++)
                (*connection_info->header)[i->first] = i->second;
            connection_info->msg_def  = (*connection_info->header)["message_definition"];
            connection_info->datatype = (*connection_info->header)["type"];
            connection_info->md5sum   = (*connection_info->header)["md5sum"];
            connections_[id] = connection_info;

            logDebug("Read CONNECTION: topic=%s id=%d", topic.c_str(), id);
        }
    }

    void Bag::readMessageDefinitionRecord102() {
        ros::Header header;
        uint32_t data_size;
        if (!readHeader(header) || !readDataLength(data_size))
            throw BagFormatException("Error reading message definition header");
        M_string& fields = *header.getValues();

        if (!isOp(fields, OP_MSG_DEF))
            throw BagFormatException("Expected MSG_DEF op not found");

        string topic, md5sum, datatype, message_definition;
        readField(fields, TOPIC_FIELD_NAME,               true, topic);
        readField(fields, MD5_FIELD_NAME,   32,       32, true, md5sum);
        readField(fields, TYPE_FIELD_NAME,                true, datatype);
        readField(fields, DEF_FIELD_NAME,    0, UINT_MAX, true, message_definition);

        ConnectionInfo* connection_info;

        map<string, uint32_t>::const_iterator topic_conn_id_iter = topic_connection_ids_.find(topic);
        if (topic_conn_id_iter == topic_connection_ids_.end()) {
            uint32_t id = connections_.size();

            logDebug("Creating connection: topic=%s md5sum=%s datatype=%s", topic.c_str(), md5sum.c_str(), datatype.c_str());
            connection_info = new ConnectionInfo();
            connection_info->id       = id;
            connection_info->topic    = topic;

            connections_[id] = connection_info;
            topic_connection_ids_[topic] = id;
        }
        else
            connection_info = connections_[topic_conn_id_iter->second];

        connection_info->msg_def  = message_definition;
        connection_info->datatype = datatype;
        connection_info->md5sum   = md5sum;
        connection_info->header = boost::make_shared<ros::M_string>();
        (*connection_info->header)["type"]               = connection_info->datatype;
        (*connection_info->header)["md5sum"]             = connection_info->md5sum;
        (*connection_info->header)["message_definition"] = connection_info->msg_def;

        logDebug("Read MSG_DEF: topic=%s md5sum=%s datatype=%s", topic.c_str(), md5sum.c_str(), datatype.c_str());
    }

    void Bag::decompressChunk(uint64_t chunk_pos) const {
        if (curr_chunk_info_.pos == chunk_pos) {
            current_buffer_ = &outgoing_chunk_buffer_;
            return;
        }

        current_buffer_ = &decompress_buffer_;

        if (decompressed_chunk_ == chunk_pos)
            return;

        // Seek to the start of the chunk
        seek(chunk_pos);

        // Read the chunk header
        ChunkHeader chunk_header;
        readChunkHeader(chunk_header);

        // Read and decompress the chunk.  These assume we are at the right place in the stream already
        if (chunk_header.compression == COMPRESSION_NONE)
            decompressRawChunk(chunk_header);
        else if (chunk_header.compression == COMPRESSION_BZ2)
            decompressBz2Chunk(chunk_header);
        else if (chunk_header.compression == COMPRESSION_LZ4)
            decompressLz4Chunk(chunk_header);
        else
            throw BagFormatException("Unknown compression: " + chunk_header.compression);

        decompressed_chunk_ = chunk_pos;
    }

    void Bag::readMessageDataRecord102(uint64_t offset, ros::Header& header) const {
        logDebug("readMessageDataRecord: offset=%llu", (unsigned long long) offset);

        seek(offset);

        uint32_t data_size;
        uint8_t op;
        do {
            if (!readHeader(header) || !readDataLength(data_size))
                throw BagFormatException("Error reading header");

            readField(*header.getValues(), OP_FIELD_NAME, true, &op);
        }
        while (op == OP_MSG_DEF);

        if (op != OP_MSG_DATA)
            throw BagFormatException((format("Expected MSG_DATA op, got %d") % op).str());

        record_buffer_.setSize(data_size);
        file_.read((char*) record_buffer_.getData(), data_size);
    }

// Reading this into a buffer isn't completely necessary, but we do it anyways for now
    void Bag::decompressRawChunk(ChunkHeader const& chunk_header) const {
        assert(chunk_header.compression == COMPRESSION_NONE);
        assert(chunk_header.compressed_size == chunk_header.uncompressed_size);

        logDebug("compressed_size: %d uncompressed_size: %d", chunk_header.compressed_size, chunk_header.uncompressed_size);

        decompress_buffer_.setSize(chunk_header.compressed_size);
        file_.read((char*) decompress_buffer_.getData(), chunk_header.compressed_size);

        // todo check read was successful
    }

    void Bag::decompressBz2Chunk(ChunkHeader const& chunk_header) const {
        assert(chunk_header.compression == COMPRESSION_BZ2);

        CompressionType compression = compression::BZ2;

        logDebug("compressed_size: %d uncompressed_size: %d", chunk_header.compressed_size, chunk_header.uncompressed_size);

        chunk_buffer_.setSize(chunk_header.compressed_size);
        file_.read((char*) chunk_buffer_.getData(), chunk_header.compressed_size);

        decompress_buffer_.setSize(chunk_header.uncompressed_size);
        file_.decompress(compression, decompress_buffer_.getData(), decompress_buffer_.getSize(), chunk_buffer_.getData(), chunk_buffer_.getSize());

        // todo check read was successful
    }

    void Bag::decompressLz4Chunk(ChunkHeader const& chunk_header) const {
        assert(chunk_header.compression == COMPRESSION_LZ4);

        CompressionType compression = compression::LZ4;

        logDebug("lz4 compressed_size: %d uncompressed_size: %d",
                 chunk_header.compressed_size, chunk_header.uncompressed_size);

        chunk_buffer_.setSize(chunk_header.compressed_size);
        file_.read((char*) chunk_buffer_.getData(), chunk_header.compressed_size);

        decompress_buffer_.setSize(chunk_header.uncompressed_size);
        file_.decompress(compression, decompress_buffer_.getData(), decompress_buffer_.getSize(), chunk_buffer_.getData(), chunk_buffer_.getSize());

        // todo check read was successful
    }

    ros::Header Bag::readMessageDataHeader(IndexEntry const& index_entry) {
        ros::Header header;
        uint32_t data_size;
        uint32_t bytes_read;
        switch (version_)
        {
            case 200:
                decompressChunk(index_entry.chunk_pos);
                readMessageDataHeaderFromBuffer(*current_buffer_, index_entry.offset, header, data_size, bytes_read);
                return header;
            case 102:
                readMessageDataRecord102(index_entry.chunk_pos, header);
                return header;
            default:
                throw BagFormatException((format("Unhandled version: %1%") % version_).str());
        }
    }

// NOTE: this loads the header, which is unnecessary
    uint32_t Bag::readMessageDataSize(IndexEntry const& index_entry) const {
        ros::Header header;
        uint32_t data_size;
        uint32_t bytes_read;
        switch (version_)
        {
            case 200:
                decompressChunk(index_entry.chunk_pos);
                readMessageDataHeaderFromBuffer(*current_buffer_, index_entry.offset, header, data_size, bytes_read);
                return data_size;
            case 102:
                readMessageDataRecord102(index_entry.chunk_pos, header);
                return record_buffer_.getSize();
            default:
                throw BagFormatException((format("Unhandled version: %1%") % version_).str());
        }
    }


    void Bag::readChunkInfoRecord() {
        // Read a CHUNK_INFO header
        ros::Header header;
        uint32_t data_size;
        if (!readHeader(header) || !readDataLength(data_size))
            throw BagFormatException("Error reading CHUNK_INFO record header");
        M_string& fields = *header.getValues();
        if (!isOp(fields, OP_CHUNK_INFO))
            throw BagFormatException("Expected CHUNK_INFO op not found");

        // Check that the chunk info version is current
        uint32_t chunk_info_version;
        readField(fields, VER_FIELD_NAME, true, &chunk_info_version);
        if (chunk_info_version != CHUNK_INFO_VERSION)
            throw BagFormatException((format("Expected CHUNK_INFO version %1%, read %2%") % CHUNK_INFO_VERSION % chunk_info_version).str());

        // Read the chunk position, timestamp, and topic count fields
        ChunkInfo chunk_info;
        readField(fields, CHUNK_POS_FIELD_NAME,  true, &chunk_info.pos);
        readField(fields, START_TIME_FIELD_NAME, true,  chunk_info.start_time);
        readField(fields, END_TIME_FIELD_NAME,   true,  chunk_info.end_time);
        uint32_t chunk_connection_count = 0;
        readField(fields, COUNT_FIELD_NAME,      true, &chunk_connection_count);

        logDebug("Read CHUNK_INFO: chunk_pos=%llu connection_count=%d start=%d.%d end=%d.%d",
                 (unsigned long long) chunk_info.pos, chunk_connection_count,
                 chunk_info.start_time.sec, chunk_info.start_time.nsec,
                 chunk_info.end_time.sec, chunk_info.end_time.nsec);
        if(chunk_info.start_time < start_time_)
            start_time_ = chunk_info.start_time;
        if(chunk_info.end_time > end_time_)
            end_time_ = chunk_info.end_time;

        // Read the topic count entries
        for (uint32_t i = 0; i < chunk_connection_count; i ++) {
            uint32_t connection_id, connection_count;
            read((char*) &connection_id,    4);
            read((char*) &connection_count, 4);

            logDebug("  %d: %d messages", connection_id, connection_count);

            chunk_info.connection_counts[connection_id] = connection_count;
        }

        chunks_.push_back(chunk_info);
    }

// Record I/O

    bool Bag::isOp(M_string& fields, uint8_t reqOp) const {
        uint8_t op = 0xFF; // nonexistent op
        readField(fields, OP_FIELD_NAME, true, &op);
        return op == reqOp;
    }


//! \todo clean this up
    void Bag::readHeaderFromBuffer(Buffer& buffer, uint32_t offset, ros::Header& header, uint32_t& data_size, uint32_t& bytes_read) const {
        assert(buffer.getSize() > 8);

        uint8_t* start = (uint8_t*) buffer.getData() + offset;

        uint8_t* ptr = start;

        // Read the header length
        uint32_t header_len;
        memcpy(&header_len, ptr, 4);
        ptr += 4;

        // Parse the header
        string error_msg;
        bool parsed = header.parse(ptr, header_len, error_msg);
        if (!parsed)
            throw BagFormatException("Error parsing header");
        ptr += header_len;

        // Read the data size
        memcpy(&data_size, ptr, 4);
        ptr += 4;

        bytes_read = ptr - start;
    }

    void Bag::readMessageDataHeaderFromBuffer(Buffer& buffer, uint32_t offset, ros::Header& header, uint32_t& data_size, uint32_t& total_bytes_read) const {
        (void)buffer;
        total_bytes_read = 0;
        uint8_t op = 0xFF;
        do {
            logDebug("reading header from buffer: offset=%d", offset);
            uint32_t bytes_read;
            readHeaderFromBuffer(*current_buffer_, offset, header, data_size, bytes_read);

            offset += bytes_read;
            total_bytes_read += bytes_read;

            readField(*header.getValues(), OP_FIELD_NAME, true, &op);
        }
        while (op == OP_MSG_DEF || op == OP_CONNECTION);

        if (op != OP_MSG_DATA)
            throw BagFormatException("Expected MSG_DATA op not found");
    }

    bool Bag::readHeader(ros::Header& header) const {
        // Read the header length
        uint32_t header_len;
        read((char*) &header_len, 4);

        // Read the header
        header_buffer_.setSize(header_len);
        read((char*) header_buffer_.getData(), header_len);

        // Parse the header
        string error_msg;
        bool parsed = header.parse(header_buffer_.getData(), header_len, error_msg);
        if (!parsed)
            return false;

        return true;
    }

    bool Bag::readDataLength(uint32_t& data_size) const {
        read((char*) &data_size, 4);
        return true;
    }

    M_string::const_iterator Bag::checkField(M_string const& fields, string const& field, unsigned int min_len, unsigned int max_len, bool required) const {
        M_string::const_iterator fitr = fields.find(field);
        if (fitr == fields.end()) {
            if (required)
                throw BagFormatException("Required '" + field + "' field missing");
        }
        else if ((fitr->second.size() < min_len) || (fitr->second.size() > max_len))
            throw BagFormatException((format("Field '%1%' is wrong size (%2% bytes)") % field % (uint32_t) fitr->second.size()).str());

        return fitr;
    }

    bool Bag::readField(M_string const& fields, string const& field_name, bool required, string& data) const {
        return readField(fields, field_name, 1, UINT_MAX, required, data);
    }

    bool Bag::readField(M_string const& fields, string const& field_name, unsigned int min_len, unsigned int max_len, bool required, string& data) const {
        M_string::const_iterator fitr = checkField(fields, field_name, min_len, max_len, required);
        if (fitr == fields.end())
            return false;

        data = fitr->second;
        return true;
    }

    bool Bag::readField(M_string const& fields, string const& field_name, bool required, Time& data) const {
        uint64_t packed_time;
        if (!readField(fields, field_name, required, &packed_time))
            return false;

        uint64_t bitmask = (1LL << 33) - 1;
        data.sec  = (uint32_t) (packed_time & bitmask);
        data.nsec = (uint32_t) (packed_time >> 32);

        return true;
    }

    std::string Bag::toHeaderString(Time const* field) const {
        uint64_t packed_time = (((uint64_t) field->nsec) << 32) + field->sec;
        return toHeaderString(&packed_time);
    }


// Low-level I/O



    void Bag::read(char* b, std::streamsize n) const  { file_.read(b, n);             }
    void Bag::seek(uint64_t pos, int origin) const    { file_.seek(pos, origin);      }



} // namespace rosbag

