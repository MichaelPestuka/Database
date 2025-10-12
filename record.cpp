#include "record.hpp"
#include <any>
#include <cstdint>
#include <string>
#include <vector>
#include "bitutils.hpp"


Record::Record(DataType type, std::any value) {
    this->type = type;
    this->value = value;
}

Record::Record(uint8_t* serialized_data) {
    this->type = static_cast<DataType>(serialized_data[0]);
    switch(this->type) {
        case DataType::INTEGER:
        {
            uint64_t new_value = 0;
            for(int i = 1; i < sizeof(uint64_t) + 1; i++) {
                new_value += ((uint64_t)serialized_data[i]) << (sizeof(uint64_t) - i )*8;
            }
            this->value = new_value;
        }
        break;

        case DataType::STRING:
        {
            std::string new_value{};
            for(int i = 1; serialized_data[i] != '\0'; i++) {
                new_value.push_back(serialized_data[i]);
            }
            this->value = new_value;
        }
        break;

        default:
        {
            this->value = 0;
        }
        break;
    }
}

vector<uint8_t> Record::Serialize() {
    vector<uint8_t> serialized{this->type};
    switch (this->type) {
        case DataType::INTEGER:
        {
            for(auto c : ToCharVector(std::any_cast<uint64_t>(this->value))){
                serialized.push_back(c);
            }
            return serialized;
        }
        break;

        case DataType::STRING: {
            for(auto c : std::any_cast<std::string>(this->value))
            {
                serialized.push_back(c);
            }
            return serialized;
        }
        break;

        default:
            return std::vector<uint8_t>{};
        break;
    }
}