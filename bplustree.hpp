
#include <cstdint>
#include <deque>
#include <fstream>
#include <vector>
#include <string>

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
        uint16_t GetBytes();


        bool HasKey(vector<uint8_t> key);
        uint16_t FindIndexBefore(vector<uint8_t> key);

        BPlusNode InsertKV(vector<uint8_t> key, vector<uint8_t> value);
        BPlusNode InsertKV(vector<uint8_t> key, uint64_t pointer);
        BPlusNode DeleteKV(vector<uint8_t> key);
        BPlusNode UpdateKV(vector<uint8_t> key, vector<uint8_t> value);

        vector<uint8_t> Serialize();

        void PrintNodeData();
    
    private:
        // type | key_count | key_offsets    | value_offsets  | keys | pointers/values |
        // 1B   | 2B        | key_count * 2B | key_count * 2B | nB   | nB              |
        BNodeType type;
        vector<vector<uint8_t>> keys;
        vector<uint64_t> pointers;
        vector<vector<uint8_t>> values;            

};

// Handles Insert, Updata, Delete operations
class BPlusTree {
    public:
        BPlusTree(std::string filename);

        void Insert(vector<uint8_t> key, vector<uint8_t> value);
        void Update(vector<uint8_t> key, vector<uint8_t> value);
        void Delete(vector<uint8_t> key);

        vector<uint8_t> Get(vector<uint8_t> key);

    private:
        std::string filename;
        uint64_t root_pointer;
        uint8_t* file_start_pointer;
        uint64_t file_page_count;

};

// Handles IO
class DiskManager {
    public:
        DiskManager(std::string filename);
        ~DiskManager();
        uint8_t* GetRoot();
        BPlusNode GetNode();
        uint8_t* GetFreePage();
        void WriteNode(BPlusNode node);
    private:
        void SetFilePageCount(uint64_t n_pages);
        int file_descriptor;
        uint64_t root;
        uint64_t page_count;
        std::deque<uint8_t*> free_pages;

};