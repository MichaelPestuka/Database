#include <algorithm>
#include <cstdint>
#include <fstream>
#include <ios>
#include <iterator>
#include <string>
#include <vector>
#include <iostream>

#include "bplustree_new.hpp"

using std::vector;

class KVDatabase {
    public:
        vector<uint8_t> Get(vector<uint8_t> key);
        void Set(vector<uint8_t> key, vector<uint8_t> value);
        void Del(vector<uint8_t> key);
    //private:
        void SaveData(std::string path, vector<uint8_t> data);
};


void KVDatabase::SaveData(std::string path, vector<uint8_t> data) {
    std::ofstream f(path, std::ios::binary);
    std::ostream_iterator<uint8_t> output_iterator(f);
    std::copy(std::begin(data), std::end(data), output_iterator);
    f.flush();
}


vector<uint8_t> KVDatabase::Get(vector<uint8_t> key) {
    std::ifstream f("test.txt", std::ios::binary);
    std::vector<uint8_t> data((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    return data;
}

int main() {
    // auto database = std::make_unique<KVDatabase>();
    // vector<uint8_t> data = {0x00, 0x01, 0x02, 0x45};

    // EncodedBNode node(PAGE_SIZE);
    // node.setHeader(1, 3);
    // node.AppendKV(0, 0, std::vector<uint8_t>{'t', 'e', 's', 't'}, std::vector<uint8_t> {'v', 'a', 'l'});
    // node.AppendKV(1, 0, std::vector<uint8_t>{'a', 'b', 'c', 'd'}, std::vector<uint8_t> {'e', 'f', 'g'});
    // node.AppendKV(2, 0, std::vector<uint8_t>{'a', 'b', 'c', 'd'}, std::vector<uint8_t> {'e', 'f', 'g'});
    // node.PrintBuffer();
    // std::cout << "N bytes: " << std::dec << node.NBytes() << std::endl;

    // C disk;
    // BTree tree(disk);
    // tree.Insert(std::vector<uint8_t> {'H', 'E', 'L', 'L', 'O'}, std::vector<uint8_t> {'W', 'O', 'R', 'L', 'D'});

    BPlusNode n( BNodeType::LEAF);
    n = n.InsertKV(vector<uint8_t>{'a', 'b', 'c'}, vector<uint8_t>{'d', 'e', 'f'});
    n.PrintNodeData();
}