#include "bplustree.hpp"
#include <algorithm>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
#include <iterator>
#include <ostream>
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

BPlusNode BPlusNode::InsertKV(vector<uint8_t> key, uint8_t* pointer) {
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

BPlusNode BPlusNode::UpdateKV(vector<uint8_t> key, uint8_t* pointer) {
    for (int i = 0; i < keys.size(); i++) {
        if(keys[i] == key) {
            pointers[i] = pointer;
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
        for(int i = 0; i < keys.size(); i++) {
            serialized.push_back(cumulative_offset >> 8);
            serialized.push_back(cumulative_offset);
            cumulative_offset += 8;
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
            auto ptr_num = reinterpret_cast<intptr_t>(ptr);
            for(int i = 56; i >= 0; i -= 8) {
                serialized.push_back(ptr_num >> i);
            }
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

BPlusNode::BPlusNode(uint8_t* data) {
    this->node_pointer = data;

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
        std::copy(data + keys_start + start_offset, data + keys_start + end_offset, std::back_inserter(key));
        keys.push_back(key);
    }
    uint16_t values_offsets = 3 + (key_count + 1) * 2;
    for(int i = 0; i < key_count * 2; i += 2) {
        uint16_t start_offset = (data[i + values_offsets] << 8) + data[i + values_offsets + 1];
        uint16_t end_offset = (data[i + values_offsets + 2] << 8) + data[i + values_offsets + 3];
        if(type == BNodeType::LEAF) {
            vector<uint8_t> value;
            value.reserve(end_offset - start_offset);
            std::copy(data + keys_start + start_offset, data + keys_start + end_offset, std::back_inserter(value));
            values.push_back(value);
        }
        else {
            intptr_t ptr_num = 0;
            int offset = 0;
            for(int i = 56; i >= 0; i -= 8) {
                uint64_t new_segment = static_cast<uint64_t>(data[keys_start + start_offset + offset]) << i;
                ptr_num += new_segment;
                offset++;
            }
            pointers.push_back(reinterpret_cast<uint8_t*>(ptr_num));
        }
    }

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
        if(type == BNodeType::LEAF) {
            vector<uint8_t> value;
            value.reserve(end_offset - start_offset);
            std::copy(data.begin() + keys_start + start_offset, data.begin() + keys_start + end_offset, std::back_inserter(value));
            values.push_back(value);
        }
        else {
            uint64_t ptr_num = 0;
            ptr_num += data[keys_start + start_offset] << 24;
            ptr_num += data[keys_start + start_offset + 1] << 16;
            ptr_num += data[keys_start + start_offset + 2] << 8;
            ptr_num += data[keys_start + start_offset + 3];
            pointers.push_back(reinterpret_cast<uint8_t*>(ptr_num));
        }
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
        if(type == BNodeType::LEAF) {
            std::cout << ", Value: ";
            for(auto c : values[i]) {
                std::cout << std::hex << c;
            }
            std::cout << "\n";
        }
        else {
            std::cout << ", Pointer: ";
            std::cout << std::hex << (uint64_t)pointers[i];
            std::cout << "\n";
        }
    }
    std::cout << std::endl;
}

// BPlusTree


BPlusTree::BPlusTree(std::string filename) : manager(filename) {
    BPlusNode root_node(BNodeType::LEAF);
    // auto first_leaf = manager.WriteNode(BPlusNode(BNodeType::LEAF));
    // root_node = root_node.InsertKV({0}, {0, 0}); // sentinel key
    root_pointer = manager.WriteNode(root_node);
}

vector<uint8_t> BPlusTree::Get(vector<uint8_t> key) {

}

// Finds a node that could contain the key
BPlusNode BPlusTree::LeafSearch(vector<uint8_t> key, BPlusNode node) {
    if(node.type == BNodeType::LEAF) {
        return  node;
    }
    int i = 0;
    for(auto k : node.keys) {
        if(k <= key) {
            return LeafSearch(key, manager.GetNode(node.pointers[i]));
        }
        i++;
    }
    return LeafSearch(key, manager.GetNode(node.pointers.back()));
}

vector<BPlusNode> BPlusTree::SplitNode(BPlusNode node) {
    if(node.GetBytes() <= 4096) {
        return {node};
    }
    
    
    BPlusNode first(node.type), second(node.type);
    int first_size, second_size;
    for(int i = 0; i < node.keys.size(); i++) {
        if(i < node.keys.size() / 2) {
            if(node.type == BNodeType::LEAF) {
                first = first.InsertKV(node.keys[i], node.values[i]);
                first_size += node.keys[i].size() + node.values[i].size();
            }
            else {
                first = first.InsertKV(node.keys[i], node.pointers[i]);
                first_size += node.keys[i].size() + 8;
            }
        }
        else {
            if(node.type == BNodeType::LEAF) {
                second = second.InsertKV(node.keys[i], node.values[i]);
                second_size += node.keys[i].size() + node.values[i].size();
            }
            else {
                second = second.InsertKV(node.keys[i], node.pointers[i]);
                second_size += node.keys[i].size() + 8;
            }
        }
    }
    return  {first, second};

}

void BPlusTree::Insert(vector<uint8_t> key, vector<uint8_t> value) {
    auto new_children = RecursiveInsert(manager.GetNode(root_pointer), key, value);
    if(new_children.size() == 1) {
        root_pointer = new_children[0].node_pointer;
    }
    else {
        BPlusNode new_root(BNodeType::NODE);
        for(auto nc : new_children) {
            auto key = nc.keys[0];
            new_root = new_root.InsertKV(key, nc.node_pointer);
        }
        root_pointer = manager.WriteNode(new_root);
    }
    // Else split root TODO
}

vector<BPlusNode> BPlusTree::RecursiveInsert(BPlusNode node, vector<uint8_t> key, vector<uint8_t> value) {
    if(node.type == BNodeType::LEAF) {
        if(node.keys.size() == 0) {
            node = node.InsertKV(key, value);
            node.node_pointer = manager.WriteNode(node);
            return {node};
        }
        // auto new_node = node.InsertKV(key, value);
        if(node.HasKey(key)) {
            node = node.UpdateKV(key, value);
        }
        else {
            node = node.InsertKV(key, value);
        }
        auto new_nodes = SplitNode(node);
        for(auto& n : new_nodes) {
            n.node_pointer = manager.WriteNode(n);
        }
        return new_nodes;
    }
    for(int i = node.keys.size() - 1; i >= 0; i--) {
        if(node.keys[i] <= key) {
            auto new_nodes = RecursiveInsert(manager.GetNode(node.pointers[i]), key, value);
            node = node.UpdateKV(node.keys[i], new_nodes[0].node_pointer);
            for(int j = 1; j < new_nodes.size(); j++) {
                node = node.InsertKV(new_nodes[j].keys.front(), new_nodes[j].node_pointer);
            }
            auto split_nodes = SplitNode(node);
            for(auto& n : split_nodes) {
                n.node_pointer = manager.WriteNode(n);
            }
            return split_nodes;
        }
    }
    std::cerr << "Shits fucked in RecursiveInsert" << std::endl; // TODO
    return {nullptr};
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
        for(int i = page_count; i < n_pages; i++) {
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

uint8_t* DiskManager::WriteNode(BPlusNode node) {
    auto page = GetFreePage();
    auto node_data = node.Serialize();
    std::copy(node_data.begin(), node_data.end(), page);
    return page;
}

BPlusNode DiskManager::GetNode(uint8_t* pointer) {
    return BPlusNode(pointer);
}

void BPlusTree::PrintTree() {
    PrintTreeRecursive(manager.GetNode(root_pointer));
}

void BPlusTree::PrintTreeRecursive(BPlusNode node) {
    if(node.type == BNodeType::LEAF) {
        node.PrintNodeData();
        return;
    }
    node.PrintNodeData();
    for(auto p : node.pointers) {
        manager.GetNode(p).PrintNodeData();
    }
}