#include <string>
#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

// #include "bplustree.hpp"
// #include "table.hpp"
#include "database.hpp"



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

    {
    DB database("./data");

    std::cout << "Loaded: " << std::endl;
    database.storage.PrintTree();

    Table tab = {"testtable", 10, {INTEGER, INTEGER, STRING}, {"one", "two", "three"}};
    Table tab2 = {"stringous", 69, {STRING, STRING, STRING}, {"uno", "dos", "tres"}};

    database.CreateTable(tab);
    database.CreateTable(tab2);
    // database.InsertRow("testtable", 1, {(uint64_t)1, (uint64_t)2, std::string("three")});
    // database.InsertRow("stringous", 4, {std::string("banan"), std::string("tastes like"), std::string("three")});
    // database.InsertRow("stringous", 6, {std::string("banana"), std::string("tastes like"), std::string("three")});
    // database.InsertRow("stringous", 9, {std::string("bananb"), std::string("tastes like"), std::string("three")});
    for(int i = 0; i < 100; i++) {
        database.InsertRow("stringous", 10 + i, {std::string("bananb"), std::string("tastes like"), std::string("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")});
    }
    // database.storage.PrintTree();
    // for(int i = 0; i < 10; i++) {
    //     database.GetRow("stringous", 10+i);
    //     database.DeleteRow("stringous", 10+i);
    // }
    // database.InsertRow("stringous", 10, {std::string("gongaga"), std::string("tastes like"), std::string("three")});
    std::cout << "Modified" << std::endl;
    database.storage.PrintTree();
    }

    std::cout << "Reopening ---------------------------------------------------------------------" << std::endl;
    DB database("./data");
    database.storage.PrintTree();

    std::cout << "yea" << std::endl;
    remove("./data");
}
