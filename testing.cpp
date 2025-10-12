#include <cstdint>
#include <ios>
#include <string>
#include <vector>
#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

// #include "bplustree.hpp"
// #include "table.hpp"
#include "database.hpp"

using std::vector;


int main() {
    // BPlusTree tree("./data", 4);
    // vector<uint8_t> long_val = {};
    // long_val.resize(512);
    // for(int i = 0; i < 16; i++) {
    //     long_val[0] = (uint8_t)(i + 65);
    //     tree.Insert({'a', (uint8_t)(i + 65)}, long_val);
    // }
    // tree.Delete({'a', 'A'});
    // tree.PrintTree();
    // auto vec = tree.Get({'a', 'A'});
    // for(auto v : vec) {
    //     std::cout << std::hex << (uint64_t)v << " ";
    // }
    // std::cout << std::endl;

    DB database("./data");

    Table tab = {"testtable", 10, {INTEGER, INTEGER, STRING}, {"one", "two", "three"}};
    Table tab2 = {"stringous", 69, {STRING, STRING, STRING}, {"uno", "dos", "tres"}};

    database.CreateTable(tab);
    database.CreateTable(tab2);
    database.InsertRow("testtable", 1, {(uint64_t)1, (uint64_t)2, std::string("three")});
    database.InsertRow("stringous", 4, {std::string("banan"), std::string("tastes like"), std::string("three")});
    database.InsertRow("stringous", 6, {std::string("banana"), std::string("tastes like"), std::string("three")});
    database.InsertRow("stringous", 9, {std::string("bananb"), std::string("tastes like"), std::string("three")});
    vector<std::any> recovered =  database.GetRow("testtable", 1);
    vector<std::any> recovered2 =  database.GetRow("stringous", 4);
    vector<std::any> recovered3 =  database.GetRow("stringous", 6);
    vector<std::any> recovered4 =  database.GetRow("stringous", 9);

    std::cout << "yea" << std::endl;
}
