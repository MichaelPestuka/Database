#include "bplustree.hpp"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iomanip>
#include <vector>
#include <iostream>


class BNode {
    public:
        std::vector<uint8_t> Encode();
    private:
        std::vector<std::vector<uint8_t>> keys;
        std::vector<std::vector<uint8_t>> vals;
        std::vector<BNode*> children;
};

/*
    Encoded structure - | type | nkeys | pointers   | offsets    | key-values |
                        | 1B   | 2B    | nkeys * 8B | nkeys * 2B | ...        |

    type - 0 - node that has children, 1 - node that has values

    KV pair structure - | key size | val size | key | val |
                        | 2B       | 2B       | ... | ... |
    
    sizes are in bytes
*/

void EncodedBNode::PrintBuffer() {
    std::cout << "Printing... " << this->data.size() << std::endl;
    int i = 1;
    for(auto b: this->data) {
        if(b == '\0') {
            std::cout << "__ ";
        }
        else
        {
            std::cout << std::setw(2) << std::setfill('0') << std::hex << (int)b << ' ';
        }
        if(i == 64) {
            std::cout << std::endl;
            i = 0;
        }
        i++;
    }
}

EncodedBNode::EncodedBNode(uint16_t buffer_size) {
    this->data = std::vector<uint8_t> (buffer_size);
}

uint8_t EncodedBNode::getType() {
    return this->data.data()[0];
}

uint16_t EncodedBNode::nKeys() {
    return (this->data.data()[1] << 8) + this->data.data()[2];
}


void EncodedBNode::setHeader(uint8_t type, uint16_t nkeys) {
    this->data.data()[0] = type;
    this->data.data()[1] = nkeys >> 8;
    this->data.data()[2] = nkeys;
}

uint64_t EncodedBNode::getPointer(uint16_t index) {
    if(index >= this->nKeys()) {
        return 0;
    }
    index = 3 + 8 * index; // calculate pointer location
    uint64_t ptr = 0;
    for(int i = 0; i < 8; i++) {
        ptr += this->data.data()[i + index] << 8 * (7 - i);
    }
    return  ptr;
}

void EncodedBNode::setPointer(uint16_t index, uint64_t ptr) {
    if(index >= this->nKeys()) {
        return; // Handle error TODO
    }
    index = 3 + 8 * index; // calculate pointer location
    for(int i = 0; i < 8; i++) {
        this->data.data()[i + index] = ptr << 8 * (7 - i);
    }
    return;
}

uint16_t EncodedBNode::GetOffset(uint16_t index) {
    if(index > this->nKeys() || index == 0) {
        return 0;
    }
    uint16_t data_index = 3 + 8 * this->nKeys() + (index - 1) * 2;
    return (this->data.data()[data_index] << 8) + this->data.data()[data_index + 1];
}

void EncodedBNode::SetOffset(uint16_t index, uint16_t offset) {
    if(index > this->nKeys()) {
        return; // TODO handle error
    }
    uint16_t data_index = 3 + 8 * this->nKeys() + (index - 1) * 2;
    this->data.data()[data_index] = offset >> 8;
    this->data.data()[data_index + 1] = offset;
    return;
}

uint16_t EncodedBNode::KvPos(uint16_t index) {
    if(index >= this->nKeys()) {
        return 0;
    }
    return 3 + 8 * this->nKeys() + 2 * this->nKeys() + this->GetOffset(index);
}

std::vector<uint8_t> EncodedBNode::GetKey(uint16_t index) {
    if(index >= this->nKeys()) {
        return std::vector<uint8_t>{};
    }

    uint16_t data_index = this->KvPos(index); // Start of KV pair
    uint16_t key_len = ((this->data.data()[data_index]) << 8) + this->data.data()[data_index + 1]; // Read key length
    std::vector<uint8_t> key {};
    std::copy_n(data.begin() + data_index + 4, key_len, key.begin()); // Copy key to new vector

    return key;
}

std::vector<uint8_t> EncodedBNode::GetVal(uint16_t index) {
    if(index >= this->nKeys()) {
        return std::vector<uint8_t>{};
    }

    uint16_t data_index = this->KvPos(index);
    uint16_t key_len = ((this->data.data()[data_index]) << 8) + this->data.data()[data_index + 1]; // Read key length
    uint16_t val_len = ((this->data.data()[data_index + 2]) << 8) + this->data.data()[data_index + 3]; // Read value length
    std::vector<uint8_t> val {};
    std::copy_n(data.begin() + data_index + 4 + key_len, val_len, val.begin()); // Copy val to new vector

    return val;
}

void EncodedBNode::AppendKV(uint16_t index, uint64_t ptr, std::vector<uint8_t> key, std::vector<uint8_t> val) {
    this->setPointer(index, ptr);
    uint16_t pos = this->KvPos(index);
    this->data.data()[pos] = key.size() >> 8;
    this->data.data()[pos+1] = key.size();
    
    this->data.data()[pos+2] = val.size() >> 8;
    this->data.data()[pos+3] = val.size();

    this->SetOffset(index + 1, this->GetOffset(index) + 4 + key.size() + val.size());
    std::copy_n(key.begin(), key.size(), this->data.begin() + pos + 4);
    std::copy_n(val.begin(), val.size(), this->data.begin() + pos + 4 + key.size());
}

uint16_t EncodedBNode::NBytes() {
    return 3 + this->nKeys() * 8 + this->nKeys() * 2 + this->GetOffset(this->nKeys()); 
}

EncodedBNode EncodedBNode::leafInsert(uint16_t index, std::vector<uint8_t> key, std::vector<uint8_t> val) {
    EncodedBNode new_node(this->data.size());

    new_node.setHeader(1, this->nKeys() + 1);
    new_node.appendRange(*this, 0, 0, index);
    new_node.AppendKV(index, 0, key, val);
    new_node.appendRange(*this, index + 1, index,  this->nKeys() - index);
    return  new_node;
}

EncodedBNode EncodedBNode::leafupdate(uint16_t index, std::vector<uint8_t> key, std::vector<uint8_t> val) {
    EncodedBNode new_node(this->data.size());

    new_node.setHeader(1, this->nKeys() + 1);
    new_node.appendRange(*this, 0, 0, index);
    new_node.AppendKV(index, 0, key, val);
    new_node.appendRange(*this, index + 1, index,  this->nKeys() - (index + 1));
    return  new_node;
}

EncodedBNode EncodedBNode::LeafDelete(uint16_t index) {
    EncodedBNode new_node(this->data.size());
    this->appendRange(*this, 0, 0, index);
    this->appendRange(*this, index, index+1, this->nKeys()-(index+1));
    return  new_node;
}

uint16_t EncodedBNode::NodeLookup(std::vector<uint8_t> key) {
    for (int i = 0; i < this->nKeys(); i++) {
        if(this->GetKey(i) > key) {
            return i - 1;
        }
        if(this->GetKey(i) == key) {
            return i;
        }
    }
    return this->nKeys() - 1;
}

void EncodedBNode::appendRange(EncodedBNode source_node, uint16_t dst_index, uint16_t src_index, uint16_t n) {
    for (int i = 0; i < n; i++) {
        uint16_t dst = dst_index + i;
        uint16_t src = src_index + i;
        this->AppendKV(dst, source_node.getPointer(src), source_node.GetKey(src), source_node.GetVal(src));
    }
}


std::vector<EncodedBNode> EncodedBNode::NodeSplit() {
    int nleft = this->nKeys()/2;
    while(3 + 8*nleft + 2*nleft + this->GetOffset(nleft) > PAGE_SIZE) {
        nleft--;
    }
    while(this->NBytes() - 3 + 8*nleft + 2*nleft + this->GetOffset(nleft) + 3 > PAGE_SIZE) {
        nleft++;
    }
    uint16_t nright = this->nKeys() - nleft;

    std::vector<EncodedBNode> split = {EncodedBNode(PAGE_SIZE * 2), EncodedBNode(PAGE_SIZE)};
    split[0].setHeader(this->getType(), nleft);
    split[1].setHeader(this->getType(), nright);
    split[0].appendRange(*this, 0, 0, nleft);
    split[1].appendRange(*this, 0, nleft, nright);

    return split;
}

std::vector<EncodedBNode> EncodedBNode::FullNodeSplit() {
    if(this->NBytes() <= PAGE_SIZE) {
        return std::vector<EncodedBNode> {*this};
    }
    EncodedBNode left(PAGE_SIZE * 2);
    EncodedBNode right(PAGE_SIZE);

    // split 2
    auto first_split = this->NodeSplit();
    if(first_split[0].NBytes() < PAGE_SIZE) {
        return first_split;
    }
    auto second_split = first_split[0].NodeSplit();

    return std::vector<EncodedBNode>{second_split[0], second_split[1], first_split[1]};
}


EncodedBNode BTree::TreeInsert(EncodedBNode node, std::vector<uint8_t> key, std::vector<uint8_t> val)
{
    uint16_t index = node.NodeLookup(key);

    if(node.getType() == NodeType::NODE) {
            uint64_t child_ptr = node.getPointer(index);
            EncodedBNode child_node = this->TreeInsert(this->GetPage(child_ptr), key, val);
            std::vector<EncodedBNode> split_child = child_node.FullNodeSplit();
            this->DeletePage(child_ptr); 
    }
    else { // Leaf
        if(key == node.GetKey(index)) {
            node.leafupdate(index, key, val);
        }
        else {
            node.leafInsert(index + 1, key, val);
        }
    }
    return node;
}


void BTree::ReplaceChildren(EncodedBNode node, uint16_t index, std::vector<EncodedBNode> children)
{
    EncodedBNode new_node(node.data.size());
    new_node.setHeader(NodeType::NODE, node.nKeys() + children.size() - 1);
    node.appendRange(node, 0, 0, index);
    for(auto c: children) {
        node.AppendKV(index, this->NewPage(c), c.GetKey(0), std::vector<uint8_t>{});
    }
    node.appendRange(node, index + children.size(), index + 1, node.nKeys()-(index + 1));
}

void BTree::Insert(std::vector<uint8_t> key, std::vector<uint8_t> val) {
    if(this->root == 0) {
        EncodedBNode root_node(PAGE_SIZE * 2);
        root_node.setHeader(NodeType::LEAF, 2);
        root_node.AppendKV(0, 0, std::vector<uint8_t>{}, std::vector<uint8_t>{});
        root_node.AppendKV(1, 0, key, val);
        this->root = NewPage(root_node);
        return;
    }
    EncodedBNode node = this->TreeInsert(this->GetPage(this->root), key, val);
    std::vector<EncodedBNode> split = node.FullNodeSplit();
    this->DeletePage(this->root);
    if(split.size() > 1) {
        EncodedBNode root_node(PAGE_SIZE * 2);
        root_node.setHeader(NodeType::NODE, split.size());
        int i = 0;
        for(auto n: split) {
            uint64_t ptr = this->NewPage(n);
            std::vector<uint8_t> key = n.GetKey(0);
            root_node.AppendKV(i, ptr, key, std::vector<uint8_t>{});
            i++;
        }
        this->root = this->NewPage(root_node);
    }
    else {
        this->root = this->NewPage(split[0]);
    }
    return;

}


std::tuple<int, EncodedBNode> BTree::ShouldMerge(EncodedBNode node, uint16_t index, EncodedBNode updated) {
    if(updated.NBytes() > PAGE_SIZE / 4) {
        return {0, EncodedBNode(PAGE_SIZE)};

    }
    if(index > 0) {
        EncodedBNode sibling = this->GetPage(node.getPointer(index - 1));
        uint16_t merged = sibling.NBytes() + updated.NBytes() - 3;
        if(merged <= PAGE_SIZE) {
            return {-1, sibling};
        }   
    }
    if(index + 1 < node.nKeys()) {
        EncodedBNode sibling = this->GetPage(node.getPointer(index + 1));

        uint16_t merged = sibling.NBytes() + updated.NBytes() - 3;
        if(merged <= PAGE_SIZE) {
            return {1, sibling};
        }   
    }
    return {0, EncodedBNode(PAGE_SIZE)};
}


EncodedBNode BTree::treeDelete(EncodedBNode node, std::vector<uint8_t> key) {
    return EncodedBNode(PAGE_SIZE);
}

EncodedBNode BTree::nodeDelete(EncodedBNode node, uint16_t index, std::vector<uint8_t> key) {
    uint64_t child_ptr = node.getPointer(index);
    EncodedBNode updated = this->treeDelete(this->GetPage(child_ptr), key);
    if(updated.nKeys() == 0) { // not found check TODO
        return updated;
    }
    this->DeletePage(child_ptr);
    EncodedBNode new_node(PAGE_SIZE * 2);
    auto merge_info = this->ShouldMerge(node, index, updated);
    if(std::get<int>(merge_info) < 0) { // left
        EncodedBNode merged = EncodedBNode::nodeMerge(std::get<EncodedBNode>(merge_info), updated);
        this->DeletePage(node.getPointer(index - 1));
        new_node = node.mergeLinks(index - 1, this->NewPage(merged), merged.GetKey(0));
    }
    else if(std::get<int>(merge_info) > 0) { // right
        EncodedBNode merged = EncodedBNode::nodeMerge(std::get<EncodedBNode>(merge_info), updated);
        this->DeletePage(node.getPointer(index + 1));
        new_node = node.mergeLinks(index, this->NewPage(merged), merged.GetKey(0));
    }
    else if(std::get<int>(merge_info) == 0 && updated.nKeys() == 0) {
        new_node.setHeader(NodeType::NODE, 0);
    }
    else {
        this->ReplaceChildren(new_node, index, std::vector<EncodedBNode>{updated});
    }
    return  new_node;;


}

C::C() {
    this->pages = {};  
}

BTree::BTree(C disk) {
    this->disk = disk;
}

uint64_t BTree::NewPage(EncodedBNode node) {
    assert(node.NBytes() < PAGE_SIZE);
    uint64_t ptr = reinterpret_cast<uint64_t>(&node);
    assert(this->disk.pages.find(ptr) != disk.pages.end());
    this->disk.pages.insert({ptr, node});
    return ptr;
}

EncodedBNode BTree::GetPage(uint64_t pointer) {
    return this->disk.pages.at(pointer);
}

void BTree::DeletePage(uint64_t pointer) {
    this->disk.pages.erase(pointer);
}