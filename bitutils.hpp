#include <cstdint>
#include <vector>
using std::vector;

template<typename T> vector<uint8_t> ToCharVector(T value) {
    vector<uint8_t> serialized(sizeof(T));
    for(int i = 0; i < sizeof(T); i++) {
        serialized[i] = value >> (sizeof(T) - i - 1)*8;
    }
    return serialized;
}

template<typename T> T FromCharVector(vector<uint8_t> serialized) {
    T value{};
    for(int i = 0; i < sizeof(T); i++) {
        value += ((T)serialized[i]) << (sizeof(T) - i - 1)*8;
    }
    return value;
}