#include "database.hpp"
#include <algorithm>
#include <any>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <ostream>
#include <string>
#include <sys/types.h>
#include <vector>
#include "bitutils.hpp"

DB::DB(std::string filename) : 
    storage(filename, 4),
    meta_table("@meta", 1, {DataType::STRING, DataType::STRING}, {"key", "val"}),
    table_schema_table("@table", 2, {DataType::STRING, DataType::STRING}, {"name", "def"}) {

    storage.Insert({'@', 'm', 'e', 't', 'a'}, meta_table.SerializeTableSchema());
    storage.Insert({'@', 't', 'a', 'b', 'l', 'e'}, table_schema_table.SerializeTableSchema());
}

void DB::CreateTable(Table table) {
    Table table_schema_table(storage.Get({'@', 't', 'a', 'b', 'l', 'e'}).data());
    vector<uint8_t> table_def = table.SerializeTableSchema();
    vector<uint8_t> table_name({'\002'}); // Prefix
    for(auto c : table.name) {
        table_name.push_back(c);
    }
    storage.Insert(table_name, table_def);
}

/*
    Schema : | n_records | rec_1 | \0 | rec_2 | ... | \0 |
*/
void DB::InsertRow(std::string table_name, uint32_t primary_key, vector<std::any> values) {
    vector<uint8_t> prefixed_table_name({'\002'});
    for(auto c : table_name) {
        prefixed_table_name.push_back(c);
    }
    Table table(storage.Get(prefixed_table_name).data());
    
    if(!table.CheckSchema(values)) {
        std::cerr << "Bad schema insert to table " << table_name << std::endl;
        return;
    }

    vector<uint8_t> serialized_row{(uint8_t)values.size()};

    for(int i = 0; i < values.size(); i++) {
        vector<uint8_t> serialized_record = Record(table.schema[i], values[i]).Serialize();
        serialized_row.insert(serialized_row.end(), serialized_record.begin(), serialized_record.end());
        serialized_row.push_back('\0');
    }

    vector<uint8_t> prefixed_key(ToCharVector(table.prefix));
    for(auto c : ToCharVector(primary_key)) {
        prefixed_key.push_back(c);
    }

    storage.Insert(prefixed_key, serialized_row);
}

void DB::DeleteRow(std::string table_name, uint32_t primary_key) {
    storage.Delete(GetPrefixedKey(table_name, primary_key));
}

vector<std::any> DB::GetRow(std::string table_name, uint32_t primary_key) {
    vector<uint8_t> serialized = storage.Get(GetPrefixedKey(table_name, primary_key));
    uint8_t n_values = serialized[0];
    vector<std::any> deserialized;
    uint32_t current_value_pointer = 1;

    for(int i = 0; i < n_values; i++) {
        Record val(serialized.data() + current_value_pointer);
        switch (val.type) {
            case INTEGER:
            {
                current_value_pointer += sizeof(uint64_t) + 2;
            }
            break;
            case STRING:
            {
                current_value_pointer += std::any_cast<std::string>(val.value).size() + 2;
            }
            break;

            default:
                current_value_pointer += 2;
            break;
        }
        deserialized.push_back(val.value);
    }
    return  deserialized;
}

vector<uint8_t> DB::GetPrefixedKey(std::string table_name, uint32_t primary_key) {
    vector<uint8_t> prefixed_table_name({'\002'});
    std::copy(table_name.begin(), table_name.end(), std::back_inserter(prefixed_table_name));

    Table table(storage.Get(prefixed_table_name).data());

    vector<uint8_t> prefixed_key(ToCharVector(table.prefix));
    for(auto c : ToCharVector(primary_key)) {
        prefixed_key.push_back(c);
    }
    return prefixed_key;
}