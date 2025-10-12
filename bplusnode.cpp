#include "bplusnode.hpp"
#include "bitutils.hpp"
#include <iostream>
#include <sys/types.h>

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
