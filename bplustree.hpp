
#include <cstdint>
#include <vector>

using std::vector;

#define BNODE_PAGE_SIZE 4096

enum BNodeType : uint8_t {
    NODE,
    LEAF
};

class BPlusNode {
    public:
        BPlusNode(BNodeType type);
        uint16_t GetBytes();


        bool HasKey(vector<uint8_t> key);
        uint16_t FindIndexBefore(vector<uint8_t> key);

        BPlusNode InsertKV(vector<uint8_t> key, vector<uint8_t> value);
        BPlusNode InsertKV(vector<uint8_t> key, uint64_t pointer);
        BPlusNode DeleteKV(vector<uint8_t> key);
        BPlusNode UpdateKV(vector<uint8_t> key, vector<uint8_t> value);

        void PrintNodeData();
    
    private:
        // type | key_count | key_offsets    | value_offsets  | keys | pointers/values |
        // 1B   | 2B        | key_count * 2B | key_count * 2B | nB   | nB              |
        BNodeType type;
        vector<vector<uint8_t>> keys;
        vector<uint64_t> pointers;
        vector<vector<uint8_t>> values;            

};