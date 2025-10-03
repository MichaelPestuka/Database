#include <cstdint>
#include <map>
#include <string>
#include <tuple>
#include <vector>
#define PAGE_SIZE 512
#define MAX_KEY_SIZE 1024
#define MAX_VAL_SIZE 1024
#define FANOUT 100

enum NodeType {
    NODE,
    LEAF
};

class EncodedBNode {
    public:
        std::vector<uint8_t> data;
        EncodedBNode(uint16_t buffer_size);
        void setHeader(uint8_t type, uint16_t nkeys);
        uint8_t getType();
        uint16_t nKeys();
        uint64_t getPointer(uint16_t index);
        void setPointer(uint16_t index, uint64_t ptr);
        uint16_t GetOffset(uint16_t index);
        void SetOffset(uint16_t index, uint16_t offset);
        uint16_t KvPos(uint16_t index);
        std::vector<uint8_t> GetKey(uint16_t index);
        std::vector<uint8_t> GetVal(uint16_t index); 
        void AppendKV(uint16_t index, uint64_t ptr, std::vector<uint8_t> key, std::vector<uint8_t> val);
        EncodedBNode leafInsert(uint16_t index, std::vector<uint8_t> key, std::vector<uint8_t> val);
        EncodedBNode leafupdate(uint16_t index, std::vector<uint8_t> key, std::vector<uint8_t> val);
        void appendRange(EncodedBNode source_node, uint16_t dst_index, uint16_t src_index, uint16_t n);
        uint16_t NodeLookup(std::vector<uint8_t> key);
        std::vector<EncodedBNode> NodeSplit();
        std::vector<EncodedBNode> FullNodeSplit();
        EncodedBNode LeafDelete(uint16_t index);
        uint16_t NBytes();
        void PrintBuffer();

        EncodedBNode leafDelete(uint16_t index);
        static EncodedBNode nodeMerge(EncodedBNode left, EncodedBNode right);
        EncodedBNode mergeLinks(uint16_t index, uint64_t ptr, std::vector<uint8_t> key);

};


class C {
    public:
        C();
        std::map<std::string, std::string> ref;
        std::map<uint64_t, EncodedBNode> pages;
};

class BTree {
    public: 
        BTree(C disk);
        uint64_t root;
        EncodedBNode GetPage(uint64_t pointer);
        uint64_t NewPage(EncodedBNode node);
        void DeletePage(uint64_t pointer);

        EncodedBNode TreeInsert(EncodedBNode node, std::vector<uint8_t> key, std::vector<uint8_t> val);
        void ReplaceChildren(EncodedBNode node, uint16_t index, std::vector<EncodedBNode> children);

        void Insert(std::vector<uint8_t> key, std::vector<uint8_t> val);
        void Delete(std::vector<uint8_t> key);
        
        std::tuple<int, EncodedBNode> ShouldMerge(EncodedBNode node, uint16_t index, EncodedBNode updated);

        EncodedBNode nodeDelete(EncodedBNode node, uint16_t index, std::vector<uint8_t> key);
        EncodedBNode treeDelete(EncodedBNode node, std::vector<uint8_t> key);
    private:
        C disk;

};

