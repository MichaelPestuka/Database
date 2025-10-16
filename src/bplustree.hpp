#ifndef BPLUSTREE
#define BPLUSTREE

#include <cstdint>
#include <map>
#include <vector>
#include <string>

using std::vector;
using std::map;

#include "bplusnode.hpp"
#include "diskmanager.hpp"

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
        uint64_t root_pointer;
        uint64_t file_page_count;

        uint64_t branching_factor;
};

#endif