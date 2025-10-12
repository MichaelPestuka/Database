#include <cstdint>
#include <vector>
#include <string>
#include "record.hpp"

using std::vector;

class Table {
    public:
        Table(std::string table_name, uint32_t prefix, vector<DataType> column_schema, vector<std::string> column_names);
        Table(uint8_t* data);

        uint32_t prefix;
        std::string name;
        vector<DataType> schema;
        vector<std::string> column_names;

        vector<uint8_t> SerializeTableSchema();
        bool CheckSchema(vector<std::any> values);
};