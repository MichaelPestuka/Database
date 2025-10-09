#include <cstdint>
#include <ios>
#include <string>
#include <vector>
#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "bplustree.hpp"

using std::vector;


int main() {
    BPlusTree tree("./data", 4);
    vector<uint8_t> long_val = {};
    long_val.resize(512);
    for(int i = 0; i < 64; i++) {
        long_val[0] = (uint8_t)(i + 65);
        tree.Insert({'a', (uint8_t)(i + 65)}, long_val);
    }
    
    auto vec = tree.Get({'a', 'A'});
    for(auto v : vec) {
        std::cout << std::hex << (uint64_t)v << " ";
    }
    std::cout << std::endl;
}
