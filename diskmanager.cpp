#include "diskmanager.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <set>
#include <ostream>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>
#include "bitutils.hpp"
#include "bplusnode.hpp"

// Disk manager

DiskManager::DiskManager(std::string filename) {
    this->filename = filename;
    if(std::filesystem::exists(filename)) {
        file_descriptor = open(filename.c_str(), O_RDWR, S_IRUSR | S_IWUSR);
        metadata_page = static_cast<uint8_t*>(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0));
        LoadMetadata();
        metadata_page = static_cast<uint8_t*>(mmap(NULL, 4096 * page_count, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0));
        FindOrphanedNodes();
    } else {
        file_descriptor = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        page_count = 1; // one reseved for metadata
        metadata_page = nullptr;
        root = 0;
        SetFilePageCount(8);
    }
}

DiskManager::~DiskManager() {
    close(file_descriptor);
}

void DiskManager::SetFilePageCount(uint64_t n_pages) {
    if(n_pages > page_count) {
        ftruncate(file_descriptor, n_pages * 4096);
        metadata_page = static_cast<uint8_t*>(mmap(NULL, 4096 * n_pages, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0));
        for(int i = page_count; i < n_pages; i++) {
            free_pages.push_back(i * 4096);
        }
    }
    else {
        std::cerr << "Reducing number of pages would lose data" << std::endl;
    }
    page_count = n_pages;
    vector<uint8_t> serialized_page_count = ToCharVector(page_count);
    std::copy(serialized_page_count.begin(), serialized_page_count.end(), metadata_page);   
}

uint64_t DiskManager::GetFreePage() {
    if(free_pages.size() == 0) {
        SetFilePageCount(page_count * 2);
    }
    auto page = free_pages.front();
    std::fill(metadata_page + page, metadata_page + page + 4096, 0);
    free_pages.pop_front();
    return page;
}

uint64_t DiskManager::WriteNode(BPlusNode node) {
    auto page = GetFreePage();
    auto node_data = node.Serialize();
    std::copy(node_data.begin(), node_data.end(), metadata_page + page);
    msync(metadata_page + page, 4096, MS_SYNC);
    return page;
}

BPlusNode DiskManager::GetNode(uint64_t pointer) {
    BPlusNode node(metadata_page + pointer);
    node.node_pointer = pointer;
    return node;
}

void DiskManager::LoadMetadata() {
    this->page_count = FromCharPointer<uint64_t>(metadata_page);
    this->root = FromCharPointer<uint64_t>(metadata_page + sizeof(uint64_t));
}

uint64_t DiskManager::GetRoot() {
    return FromCharPointer<uint64_t>(metadata_page + sizeof(uint64_t));
}

void DiskManager::SetRoot(uint64_t new_root) {
    if(root != 0)
    {
        MarkPageAsObsolete(root);
    }
    this->root = new_root;
    vector<uint8_t> Serialized_root = ToCharVector(new_root);
    std::copy(Serialized_root.begin(), Serialized_root.end(), metadata_page + sizeof(uint64_t));
}

void DiskManager::DeleteDataFile() {
    ftruncate(file_descriptor, 0);
    close(file_descriptor);
    remove(filename.c_str());
}

// TODO concurrency
void DiskManager::MarkPageAsObsolete(uint64_t pointer) {
    free_pages.push_back(pointer);
}

void DiskManager::FindOrphanedNodes() {
    std::set<uint64_t> not_orphaned;

    std::deque<BPlusNode> searched_nodes;
    searched_nodes.push_back(GetNode(root));
    not_orphaned.insert(root);
    for(auto p : searched_nodes.front().pointer_map) {
        searched_nodes.push_back(GetNode(p.second));
        not_orphaned.insert(p.second);
    }

    for(int i = 1; i < page_count; i++) {
        if(not_orphaned.count(i * 4096) == 0) {
            MarkPageAsObsolete(i * 4096);
        }
    }
}