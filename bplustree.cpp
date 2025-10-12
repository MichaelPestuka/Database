#include "bplustree.hpp"
#include "bitutils.hpp"
#include <algorithm>
#include <cstdint>
#include <deque>
#include <fcntl.h>
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
    uint16_t key_val_sum = 0;

    for(auto key_val : value_map) {
        key_val_sum += key_val.first.size() + key_val.second.size();
    }
    for(auto key_val : pointer_map) {
        key_val_sum += key_val.first.size() + 8;
    }

    return 3 + value_map.size() * 2 + pointer_map.size() * 2 + key_val_sum;
}

bool BPlusNode::HasKey(vector<uint8_t> key) {
    if(value_map.count(key) > 0 || pointer_map.count(key) > 0) {
        return true;
    }
    return false;
}

BPlusNode BPlusNode::InsertKV(vector<uint8_t> key, vector<uint8_t> value) {
    value_map[key] = value;

    return *this;
}

BPlusNode BPlusNode::InsertKV(vector<uint8_t> key, uint8_t* pointer) {
    pointer_map[key] = pointer;

    return *this;
}

BPlusNode BPlusNode::UpdateKV(vector<uint8_t> key, vector<uint8_t> value) {
    value_map[key] = value;
    return *this;
}

BPlusNode BPlusNode::UpdateKV(vector<uint8_t> key, uint8_t* pointer) {
    pointer_map[key] = pointer;
    return *this;
}

BPlusNode BPlusNode::DeleteKV(vector<uint8_t> key) {
    if(type == BNodeType::NODE) {
        pointer_map.erase(key);
    }
    else {
        value_map.erase(key);
    }
    return *this;
}

vector<uint8_t> BPlusNode::Serialize() {
    vector<uint8_t> serialized;
    serialized.reserve(4096);

    serialized.push_back(type);
    uint16_t key_count = value_map.size() + pointer_map.size();
    serialized.push_back(key_count >> 8);
    serialized.push_back(key_count);

    vector<uint16_t> key_offsets;
    vector<uint16_t> value_offsets;
    vector<uint8_t> serialized_keys{};
    vector<uint8_t> serialized_values{};

    uint16_t cumulative_key_offset = 0;
    uint16_t cumulative_value_offset = 0;
    if(type == BNodeType::NODE) {
        for(auto key_pointer : pointer_map) {
            key_offsets.push_back(cumulative_key_offset);
            value_offsets.push_back(cumulative_value_offset);
            cumulative_key_offset += key_pointer.first.size();
            cumulative_value_offset += 8;
            serialized_keys.insert(serialized_keys.end(), key_pointer.first.begin(), key_pointer.first.end());
            auto pointer = ToCharVector(reinterpret_cast<intptr_t>(key_pointer.second));
            serialized_values.insert(serialized_values.end(), pointer.begin(), pointer.end());
        }
        key_offsets.push_back(cumulative_key_offset);
        value_offsets.push_back(cumulative_value_offset);

        // Start serializing
        for(auto ko : key_offsets) {
            auto key_offset = ToCharVector(ko);
            serialized.insert(serialized.end(), key_offset.begin(), key_offset.end());
        }
        for(auto vo : value_offsets) {
            auto value_offset = ToCharVector(static_cast<uint16_t>(vo + key_offsets.back()));
            serialized.insert(serialized.end(), value_offset.begin(), value_offset.end());
        }
        serialized.insert(serialized.end(), serialized_keys.begin(), serialized_keys.end());
        serialized.insert(serialized.end(), serialized_values.begin(), serialized_values.end());
    }
    else {
        for(auto key_value : value_map) {
            key_offsets.push_back(cumulative_key_offset);
            value_offsets.push_back(cumulative_value_offset);
            cumulative_key_offset += key_value.first.size();
            cumulative_value_offset += key_value.second.size();
            serialized_keys.insert(serialized_keys.end(), key_value.first.begin(), key_value.first.end());
            serialized_values.insert(serialized_values.end(), key_value.second.begin(), key_value.second.end());
        }
        key_offsets.push_back(cumulative_key_offset);
        value_offsets.push_back(cumulative_value_offset);

        // Start serializing
        for(auto ko : key_offsets) {
            auto key_offset = ToCharVector(ko);
            serialized.insert(serialized.end(), key_offset.begin(), key_offset.end());
        }
        for(auto vo : value_offsets) {
            auto value_offset = ToCharVector(static_cast<uint16_t>(vo + key_offsets.back()));
            serialized.insert(serialized.end(), value_offset.begin(), value_offset.end());
        }
        serialized.insert(serialized.end(), serialized_keys.begin(), serialized_keys.end());
        serialized.insert(serialized.end(), serialized_values.begin(), serialized_values.end());
    }
    return serialized;
}

// Deserialize constructor
BPlusNode::BPlusNode(uint8_t* data) {
    this->node_pointer = data;

    vector<vector<uint8_t>> keys, values;
    vector<uint8_t*> pointers;

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

    if(type == BNodeType::LEAF) {
        for(int i = 0; i < key_count; i++) {
            value_map[keys[i]] = values[i];
        }
    }
    else {
        for(int i = 0; i < key_count; i++) {
            pointer_map[keys[i]] = pointers[i];
        }
    }
}

void BPlusNode::PrintNodeData() {
    std::cout << "Node type: " << (uint)type << ", Pointer: " << (uint64_t)node_pointer << "\n";
    std::cout << "KVs: \n";

    if(type == BNodeType::NODE)
    {
        for(auto key_value : pointer_map) {
            std::cout << "Key: ";
            for(auto c : key_value.first) {
                std::cout << std::hex << c;
            }
            std::cout << ", Pointer: ";
            std::cout << std::hex << (uint64_t)key_value.second;
            std::cout << "\n";
        }
    }
    else {
        for(auto key_value : value_map) {
            std::cout << "Key: ";
            for(auto c : key_value.first) {
                std::cout << std::hex << c;
            }
            std::cout << ", Value: ";
            for(auto c : key_value.second) {
                std::cout << std::hex << c;
            }
            std::cout << "\n";
        }
    }
    std::cout << std::endl;
}

// BPlusTree


BPlusTree::BPlusTree(std::string filename, uint64_t branching_factor) : manager(filename) {
    BPlusNode root_node(BNodeType::LEAF);
    // auto first_leaf = manager.WriteNode(BPlusNode(BNodeType::LEAF));
    // root_node = root_node.InsertKV({0}, {0, 0}); // sentinel key
    root_pointer = manager.WriteNode(root_node);
    this->branching_factor = branching_factor;
}

vector<uint8_t> BPlusTree::Get(vector<uint8_t> key) {
    auto node = manager.GetNode(root_pointer);
    while(node.type != BNodeType::LEAF) {
        for(auto key_val = node.pointer_map.rbegin(); key_val != node.pointer_map.rend(); key_val++) {
            if(key_val->first <= key) {
                node = manager.GetNode(key_val->second);
                break;
            }
        }
    }
    return node.value_map[key];
}

void BPlusTree::Delete(vector<uint8_t> key) {
    root_pointer = manager.WriteNode(RecursiveDelete(manager.GetNode(root_pointer), key));
}

BPlusNode BPlusTree::RecursiveDelete(BPlusNode node, vector<uint8_t> key) {
    if(node.type == BNodeType::LEAF) {
        node = node.DeleteKV(key);
        node.node_pointer = manager.WriteNode(node);
        return node;
    }
    for(auto key_val = node.pointer_map.rbegin(); key_val != node.pointer_map.rend(); key_val++)
    {
        if(key_val->first <= key) {
            auto new_node = RecursiveDelete(manager.GetNode(key_val->second), key);
            if(key_val->first != key) {
                node = node.UpdateKV(key_val->first, new_node.node_pointer);
            }
            else {
                node = node.DeleteKV(key);
                node = node.InsertKV(key_val->first, new_node.node_pointer);
            }
            node.node_pointer = manager.WriteNode(node);
            return node;
        }
    }
    std::cerr << "Shits fucked in RecursiveDelete" << std::endl; // TODO
    return {nullptr};
}

// Finds a node that could contain the key
// BPlusNode BPlusTree::LeafSearch(vector<uint8_t> key, BPlusNode node) {
//     if(node.type == BNodeType::LEAF) {
//         return  node;
//     }
//     int i = 0;
//     for(auto k : node.keys) {
//         if(k <= key) {
//             return LeafSearch(key, manager.GetNode(node.pointers[i]));
//         }
//         i++;
//     }
//     return LeafSearch(key, manager.GetNode(node.pointers.back()));
// }


vector<BPlusNode> BPlusTree::SplitNode(BPlusNode node) {
    if(node.GetBytes() <= 4096 && branching_factor > node.pointer_map.size()) {
        return {node};
    }
    
    BPlusNode first(node.type), second(node.type);
    int first_size, second_size;

    if(node.type == BNodeType::LEAF) {
        int i = 0;
        for(auto key_val : node.value_map) {
            if(i < node.value_map.size() / 2) {
                first = first.InsertKV(key_val.first, key_val.second);
            }
            else {
                second = second.InsertKV(key_val.first, key_val.second);
            }
            i++;
        }
    }
    else {
        int i = 0;
        for(auto key_val : node.pointer_map) {
            if(i < node.pointer_map.size() / 2) {
                first = first.InsertKV(key_val.first, key_val.second);
            }
            else {
                second = second.InsertKV(key_val.first, key_val.second);
            }
            i++;
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
        vector<uint8_t> key;
        for(auto nc : new_children) {
            if(nc.type == BNodeType::LEAF) {
                key = nc.value_map.begin()->first;
            }
            else {
                key = nc.pointer_map.begin()->first;
            }
            new_root = new_root.InsertKV(key, nc.node_pointer);
        }
        root_pointer = manager.WriteNode(new_root);
    }
    // Else split root TODO
}

vector<BPlusNode> BPlusTree::RecursiveInsert(BPlusNode node, vector<uint8_t> key, vector<uint8_t> value) {
    if(node.type == BNodeType::LEAF) {
        if(node.value_map.size() == 0) {
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
    for(auto key_val = node.pointer_map.rbegin(); key_val != node.pointer_map.rend(); key_val++)
    {
        if(key_val->first <= key) {
            auto new_nodes = RecursiveInsert(manager.GetNode(key_val->second), key, value);
            node = node.UpdateKV(key_val->first, new_nodes[0].node_pointer);
            for(int j = 1; j < new_nodes.size(); j++) {
                if(new_nodes[j].type == BNodeType::LEAF) {
                    node = node.InsertKV(new_nodes[j].value_map.begin()->first, new_nodes[j].node_pointer);
                }
                else {
                    node = node.InsertKV(new_nodes[j].pointer_map.begin()->first, new_nodes[j].node_pointer);
                }
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
    msync(page, 4096, MS_SYNC);
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
    for(auto p : node.pointer_map) {
        PrintTreeRecursive(manager.GetNode(p.second));
    }
}