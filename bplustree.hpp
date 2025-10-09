
#include <cstdint>
#include <deque>
#include <map>
#include <vector>
#include <string>

using std::vector;
using std::map;

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


// Handles IO
class DiskManager {
    public:
        DiskManager(std::string filename);
        ~DiskManager();
        uint8_t* GetRoot();
        BPlusNode GetNode(uint8_t* pointer);
        uint8_t* GetFreePage();
        uint8_t* WriteNode(BPlusNode node);
    private:
        void SetFilePageCount(uint64_t n_pages);
        int file_descriptor;
        uint64_t root;
        uint64_t page_count;
        std::deque<uint8_t*> free_pages;

};

// Handles Insert, Updata, Delete operations
class BPlusTree {
    public:
        BPlusTree(std::string filename, uint64_t branching_factor);

        void Insert(vector<uint8_t> key, vector<uint8_t> value);
        void Update(vector<uint8_t> key, vector<uint8_t> value);
        void Delete(vector<uint8_t> key);

        void PrintTree();

        vector<uint8_t> Get(vector<uint8_t> key);

    private:
        vector<BPlusNode> RecursiveInsert(BPlusNode node, vector<uint8_t> key, vector<uint8_t> value);
        BPlusNode RecursiveDelete(BPlusNode node, vector<uint8_t> key);
        void PrintTreeRecursive(BPlusNode node);
        BPlusNode LeafSearch(vector<uint8_t> key, BPlusNode node);

        vector<BPlusNode> SplitNode(BPlusNode node);
        BPlusNode MergeNodes(std::vector<BPlusNode> nodes);

        std::string filename;
        DiskManager manager;
        uint8_t* root_pointer;
        uint64_t file_page_count;

        uint64_t branching_factor;
};