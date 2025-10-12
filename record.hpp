#include <any>
#include <cstdint>
#include <vector>
using std::vector;

enum DataType : uint8_t {
    NONE,
    INTEGER,
    STRING
};

class Record {
    public: 
        Record(DataType type, std::any value);
        Record(uint8_t* serialzied_data);
        DataType type;
        std::any value;
        std::any Get();
        vector<uint8_t> Serialize();
};
