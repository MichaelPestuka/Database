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
        uint64_t GetRoot();
        void SetRoot(uint64_t new_root);
        BPlusNode GetNode(uint64_t pointer);
        uint64_t GetFreePage();
        uint64_t WriteNode(BPlusNode node);

        void MarkPageAsObsolete(uint64_t pointer);
        void FindOrphanedNodes();

        void DeleteDataFile();
    private:
        void SetFilePageCount(uint64_t n_pages);
        void LoadMetadata();
        std::string filename;
        int file_descriptor;

        uint8_t* metadata_page;
        uint64_t root;
        uint64_t page_count;
        deque<uint64_t> free_pages;

};

#endif