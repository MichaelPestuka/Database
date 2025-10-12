#include "table.hpp"
#include <any>
#include <cstdint>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <vector>
#include "bitutils.hpp"

Table::Table(std::string table_name, uint32_t prefix, vector<DataType> column_schema, vector<std::string> column_names) {
    this->name = table_name;
    this->prefix = prefix;
    this->schema = column_schema;
    this->column_names = column_names;
}

Table::Table(uint8_t* data) {
    this->prefix = FromCharPointer<uint32_t>(data);

    for(int i = 0; i < data[4]; i++) { // Load name
        name += data[5 + i];
    }

    uint8_t n_records = data[5 + data[4]];
    int records_start = 6 + data[4];
    for(int i = 0; i < n_records; i++) {
        this->schema.push_back(static_cast<DataType>( data[records_start + i]));
    }
    int current_name = n_records + records_start;
    for(int i = 0; i < n_records; i++) {
        std::string col_name;
        for(int j = 0; j < data[current_name]; j++) {
            col_name += data[current_name + j + 1];
        }
        current_name += data[current_name] + 1;
        this->column_names.push_back(col_name);
    }
}

/*
    | prefix | name | n_records | n * record_type | n * record_name |

    name/record_name = | len(name) | name |
*/
vector<uint8_t> Table::SerializeTableSchema() {
    vector<uint8_t> serialized = ToCharVector(prefix); // Add prefix

    serialized.push_back(name.size());
    for(auto c : name) {
        serialized.push_back(c);
    }

    serialized.push_back(schema.size()); // Add number of records

    for(auto record : schema) {
        serialized.push_back(record); // TODO more than 256 records in schema
    }

    for(auto name : column_names) {
        serialized.push_back(name.size()); // TODO longer names than 256 chars
        for(auto c : name) {
            serialized.push_back(c);
        }
    }
    return serialized;
}

bool Table::CheckSchema(vector<std::any> values) {
    if(this->schema.size() != values.size()) { // Record count mismatch
        return false;
    }
    for(int i = 0; i < this->schema.size(); i++) {
        switch (this->schema[i]) {
            case INTEGER:
                if(values[i].type() != typeid(uint64_t)) {
                    return false;
                }
            break;
            case STRING:
            {
                if(values[i].type() != typeid(std::string)) {
                    return false;
                }
            }
            break;
            default:
                
            break;
        }
    }
    return true;
}