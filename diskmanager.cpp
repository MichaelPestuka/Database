#include "diskmanager.hpp"
#include <fcntl.h>
#include <iostream>
#include <ostream>
#include <sys/mman.h>
#include <unistd.h>

// Disk manager

DiskManager::DiskManager(std::string filename) {
    file_descriptor = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    page_count = 0;
    SetFilePageCount(8);
}

DiskManager::~DiskManager() {
    close(file_descriptor);
}

void DiskManager::SetFilePageCount(uint64_t n_pages) {
    if(n_pages > page_count) {
        ftruncate(file_descriptor, n_pages * 4096);
        for(int i = page_count; i < n_pages; i++) {
            auto new_mapping = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, i * 4096));
            free_pages.push_back(new_mapping);
        }
        
    }
    else {
        std::cerr << "Reducing number of pages would lose data" << std::endl;
    }
    page_count = n_pages;
}

uint8_t* DiskManager::GetFreePage() {
    if(free_pages.size() < 5) {
        SetFilePageCount(page_count * 2);
    }
    auto page = free_pages.front();
    free_pages.pop_front();
    return page;
}

uint8_t* DiskManager::WriteNode(BPlusNode node) {
    auto page = GetFreePage();
    auto node_data = node.Serialize();
    std::copy(node_data.begin(), node_data.end(), page);
    msync(page, 4096, MS_SYNC);
    return page;
}

BPlusNode DiskManager::GetNode(uint8_t* pointer) {
    return BPlusNode(pointer);
}
