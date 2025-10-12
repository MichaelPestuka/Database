#ifndef DISKMANAGER
#define DISKMANAGER

#include <cstdint>
#include <string>
#include <deque>
#include "bplusnode.hpp"

using std::string;
using std::deque;

// Handles IO
class DiskManager {
    public:
        DiskManager(std::string filename);
        ~DiskManager();
        uint8_t* GetRoot();
        void SetRoot(uint8_t* new_root);
        BPlusNode GetNode(uint8_t* pointer);
        uint8_t* GetFreePage();
        uint8_t* WriteNode(BPlusNode node);

        void DeleteDataFile();
    private:
        void SetFilePageCount(uint64_t n_pages);
        void LoadMetadata();
        std::string filename;
        int file_descriptor;

        uint8_t* metadata_page;
        uint8_t* root;
        uint64_t page_count;
        deque<uint8_t*> free_pages;

};

#endif