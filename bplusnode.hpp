#ifndef BPLUSNODE
#define BPLUSNODE

#include <cstdint>
#include <vector>
#include <map>

using std::map;
using std::vector;

#define BNODE_PAGE_SIZE 4096

enum BNodeType : uint8_t {
    NODE,
    LEAF
};

// Handles indvidual node operations
class BPlusNode {
    public:
        BPlusNode(BNodeType type);
        BPlusNode(vector<uint8_t> data);
        BPlusNode(uint8_t* data);
        uint16_t GetBytes();


        bool HasKey(vector<uint8_t> key);
        uint16_t FindIndexBefore(vector<uint8_t> key);

        BPlusNode InsertKV(vector<uint8_t> key, vector<uint8_t> value);
        BPlusNode InsertKV(vector<uint8_t> key, uint8_t* pointer);
        BPlusNode DeleteKV(vector<uint8_t> key);
        BPlusNode UpdateKV(vector<uint8_t> key, vector<uint8_t> value);
        BPlusNode UpdateKV(vector<uint8_t> key, uint8_t* pointer);

        vector<uint8_t> Serialize();

        void PrintNodeData();

        BPlusNode MergeChildren();
    
        BNodeType type;

        map<vector<uint8_t>, vector<uint8_t>> value_map;
        map<vector<uint8_t>, uint8_t*> pointer_map;


        // vector<vector<uint8_t>> keys;
        // vector<uint8_t*> pointers;
        // vector<vector<uint8_t>> values;    
        
        uint8_t* node_pointer;
    private:
        // type | key_count | key_offsets    | value_offsets  | keys | pointers/values |
        // 1B   | 2B        | key_count * 2B | key_count * 2B | nB   | nB              |

};

#endif