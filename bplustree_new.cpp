#include "bplustree_new.hpp"
#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
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

void BPlusNode::PrintNodeData() {
    std::cout << "Node type: " << type << "\n";
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