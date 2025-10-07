#include "bplustree.hpp"
#include <algorithm>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
#include <iterator>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

BPlusNode::BPlusNode(BNodeType type) {
    this->type = type;
}

uint16_t BPlusNode::GetBytes() {
    uint16_t key_sum = 0;
    for(auto k : keys) {
        key_sum += k.size();
    }
    if(type == BNodeType::NODE) {
        return 3 + keys.size() * 2 + key_sum + pointers.size() * 8;
    }
    else {
        uint16_t value_sum = 0;
        for(auto v : values) {
            value_sum += v.size();
        }
        return 3 + keys.size() * 2 + values.size() * 2 + key_sum + value_sum;
    }
}

bool BPlusNode::HasKey(vector<uint8_t> key) {
    for(auto k : keys) {
        if(k == key) {
            return true;
        }
    }
    return false;
}

BPlusNode BPlusNode::InsertKV(vector<uint8_t> key, vector<uint8_t> value) {

    int place_at = keys.size();
    for (int i = 0; i < keys.size(); i++) {
        if(keys[i] > key) {
            place_at = i;
            break;
        }
    }

    keys.insert(keys.begin() + place_at, key);
    values.insert(values.begin() + place_at, value);

    return *this;
}

BPlusNode BPlusNode::InsertKV(vector<uint8_t> key, uint64_t pointer) {
    int place_at = keys.size();
    for (int i = 0; i < keys.size(); i++) {
        if(keys[i] > key) {
            place_at = i;
            break;
        }
    }

    keys.insert(keys.begin() + place_at, key);
    pointers.insert(pointers.begin() + place_at, pointer);
    return *this;
}

BPlusNode BPlusNode::UpdateKV(vector<uint8_t> key, vector<uint8_t> value) {
    for (int i = 0; i < keys.size(); i++) {
        if(keys[i] == key) {
            values[i] = value;
            break;
        }
    }
    return *this;
}

BPlusNode BPlusNode::DeleteKV(vector<uint8_t> key) {
    for (int i = 0; i < keys.size(); i++) {
        if(keys[i] == key) {
            values.erase(values.begin() + i);
            keys.erase(keys.begin() + i);
            break;
        }
    }
    return *this;
}

vector<uint8_t> BPlusNode::Serialize() {
    vector<uint8_t> serialized;
    serialized.reserve(4096);

    serialized.push_back(type);
    serialized.push_back(keys.size() >> 8);
    serialized.push_back(keys.size());
    // Key offsets
    uint16_t cumulative_offset = 0;
    for(auto key : keys) {
        serialized.push_back(cumulative_offset >> 8);
        serialized.push_back(cumulative_offset);
        cumulative_offset += key.size();
    }
    serialized.push_back(cumulative_offset >> 8);
    serialized.push_back(cumulative_offset);
    // Value offsets
    if(type == BNodeType::NODE) {
        for(int i = 0; i < keys.size() + 1; i++) {
            serialized.push_back(cumulative_offset >> 8);
            serialized.push_back(cumulative_offset);
            cumulative_offset += 4;
        }
        serialized.push_back(cumulative_offset >> 8);
        serialized.push_back(cumulative_offset);
    }
    else {
        for(auto value : values) {
            serialized.push_back(cumulative_offset >> 8);
            serialized.push_back(cumulative_offset);
            cumulative_offset += value.size();
        }
        serialized.push_back(cumulative_offset >> 8);
        serialized.push_back(cumulative_offset);
    }
    for(auto key : keys) {
        serialized.insert(serialized.end(), key.begin(), key.end());
    }
    if(type == BNodeType::NODE) {
        for(auto ptr : pointers) {
            serialized.push_back(ptr >> 24);
            serialized.push_back(ptr >> 16);
            serialized.push_back(ptr >> 8);
            serialized.push_back(ptr);
        }
    }
    else {
        for(auto value: values) {
            serialized.insert(serialized.end(), value.begin(), value.end());
        }
    }
    // serialized.resize(4096);

    return serialized;
}

// Deserialize constructor
BPlusNode::BPlusNode(vector<uint8_t> data) {
    type = static_cast<BNodeType>(data[0]);

    uint16_t key_count = 0;
    key_count += data[1] << 8;
    key_count += data[2];

    uint16_t keys_start = 3 + (key_count + 1) * 4;
    for(int i = 0; i < key_count * 2; i += 2) {
        uint16_t start_offset = (data[i + 3] << 8) + data[i + 4];
        uint16_t end_offset = (data[i + 5] << 8) + data[i + 6];
        vector<uint8_t> key;
        key.reserve(end_offset - start_offset);
        std::copy(data.begin() + keys_start + start_offset, data.begin() + keys_start + end_offset, std::back_inserter(key));
        keys.push_back(key);
    }
    uint16_t values_offsets = 3 + (key_count + 1) * 2;
    for(int i = 0; i < key_count * 2; i += 2) {
        uint16_t start_offset = (data[i + values_offsets] << 8) + data[i + values_offsets + 1];
        uint16_t end_offset = (data[i + values_offsets + 2] << 8) + data[i + values_offsets + 3];
        vector<uint8_t> value;
        value.reserve(end_offset - start_offset);
        std::copy(data.begin() + keys_start + start_offset, data.begin() + keys_start + end_offset, std::back_inserter(value));
        values.push_back(value);
    }
}

void BPlusNode::PrintNodeData() {
    std::cout << "Node type: " << (uint)type << "\n";
    std::cout << "KVs: \n";
    for(int i = 0; i < keys.size(); i++) {
        std::cout << "Key: ";
        for(auto c : keys[i]) {
            std::cout << std::hex << c;
        }
        std::cout << ", Value: ";
        for(auto c : values[i]) {
            std::cout << std::hex << c;
        }
        std::cout << "\n";
    }
    std::cout << std::endl;
}


BPlusTree::BPlusTree(std::string filename) {
    this->filename = filename;

    file_page_count = 16;

    int fd = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ftruncate(fd, 4096 * file_page_count); // TODO dynamic size
    file_start_pointer = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    close(fd);
}

// Disk manager

DiskManager::DiskManager(std::string filename) {
    file_descriptor = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    page_count = 0;
    SetFilePageCount(8);
}

DiskManager::~DiskManager() {
    close(file_descriptor);
}

void DiskManager::SetFilePageCount(uint64_t n_pages) {
    if(n_pages > page_count) {
        ftruncate(file_descriptor, n_pages * 4096);
        for(int i = page_count; i < page_count + n_pages; i++) {
            auto new_mapping = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, i * 4096));
            free_pages.push_back(new_mapping);
        }
        
    }
    else {
        // TODO
    }
    page_count = n_pages;
}

uint8_t* DiskManager::GetFreePage() {
    if(free_pages.size() < 5) {
        SetFilePageCount(page_count * 2);
    }
    auto page = free_pages.front();
    free_pages.pop_front();
    return page;
}

void DiskManager::WriteNode(BPlusNode node) {
    auto page = GetFreePage();
    auto node_data = node.Serialize();
    std::copy(node_data.begin(), node_data.end(), page);
}