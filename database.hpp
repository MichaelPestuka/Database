#include "bplustree.hpp"
#include "table.hpp"
#include <cstdint>
#include <vector>

class DB {
    public: 
        BPlusTree storage;
        Table meta_table;
        Table table_schema_table;

        DB(std::string filename);
        void CreateTable(Table table);
        void InsertRow(std::string table_name, uint32_t primary_key, vector<std::any> Values);
        vector<std::any> GetRow(std::string table_name, uint32_t primary_key);
};