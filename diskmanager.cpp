#include "diskmanager.hpp"
#include <algorithm>
#include <complex>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <ostream>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>
#include "bitutils.hpp"

// Disk manager

DiskManager::DiskManager(std::string filename) {
    this->filename = filename;
    if(std::filesystem::exists(filename)) {
        file_descriptor = open(filename.c_str(), O_RDWR, S_IRUSR | S_IWUSR);
        metadata_page = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0));
        LoadMetadata();
    } else {
        file_descriptor = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        page_count = 0;
        metadata_page = nullptr;
        root = nullptr;
        SetFilePageCount(8);
    }
}

DiskManager::~DiskManager() {
    close(file_descriptor);
}

void DiskManager::SetFilePageCount(uint64_t n_pages) {
    if(n_pages > page_count) {
        ftruncate(file_descriptor, n_pages * 4096);
        if(metadata_page == nullptr) {
            metadata_page = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0));
            page_count += 1;
        }
        for(int i = page_count; i < n_pages; i++) {
            auto new_mapping = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, i * 4096));
            free_pages.push_back(new_mapping);
        }
        vector<uint8_t> serialized_page_count = ToCharVector(page_count);
        std::copy(serialized_page_count.begin(), serialized_page_count.end(), metadata_page);   
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

void DiskManager::LoadMetadata() {
    this->page_count = FromCharPointer<uint64_t>(metadata_page);
    this->root = reinterpret_cast<uint8_t*>(FromCharPointer<intptr_t>(metadata_page + sizeof(uint64_t)));
}

uint8_t* DiskManager::GetRoot() {
    return reinterpret_cast<uint8_t*>(FromCharPointer<intptr_t>(metadata_page + sizeof(uint64_t)));
}

void DiskManager::SetRoot(uint8_t* new_root) {
    this->root = new_root;
    vector<uint8_t> Serialized_root = ToCharVector(reinterpret_cast<intptr_t>(new_root));
    std::copy(Serialized_root.begin(), Serialized_root.end(), metadata_page + sizeof(uint64_t));
}

void DiskManager::DeleteDataFile() {
    ftruncate(file_descriptor, 0);
    close(file_descriptor);
    remove(filename.c_str());
}