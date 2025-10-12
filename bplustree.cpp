#include "bplustree.hpp"
#include "bitutils.hpp"
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <iterator>
#include <ostream>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>


// BPlusTree

BPlusTree::BPlusTree(std::string filename, uint64_t branching_factor) : manager(filename) {
    BPlusNode root_node(BNodeType::LEAF);
    root_pointer = manager.WriteNode(root_node);
    this->branching_factor = branching_factor;
}

vector<uint8_t> BPlusTree::Get(vector<uint8_t> key) {
    auto node = manager.GetNode(root_pointer);
    while(node.type != BNodeType::LEAF) {
        for(auto key_val = node.pointer_map.rbegin(); key_val != node.pointer_map.rend(); key_val++) {
            if(key_val->first <= key) {
                node = manager.GetNode(key_val->second);
                break;
            }
        }
    }
    return node.value_map[key];
}

void BPlusTree::Delete(vector<uint8_t> key) {
    root_pointer = manager.WriteNode(RecursiveDelete(manager.GetNode(root_pointer), key));
}

BPlusNode BPlusTree::RecursiveDelete(BPlusNode node, vector<uint8_t> key) {
    if(node.type == BNodeType::LEAF) {
        node = node.DeleteKV(key);
        node.node_pointer = manager.WriteNode(node);
        return node;
    }
    for(auto key_val = node.pointer_map.rbegin(); key_val != node.pointer_map.rend(); key_val++)
    {
        if(key_val->first <= key) {
            auto new_node = RecursiveDelete(manager.GetNode(key_val->second), key);
            if(key_val->first != key) {
                node = node.UpdateKV(key_val->first, new_node.node_pointer);
            }
            else {
                node = node.DeleteKV(key);
                node = node.InsertKV(key_val->first, new_node.node_pointer);
            }
            node.node_pointer = manager.WriteNode(node);
            return node;
        }
    }
    std::cerr << "Shits fucked in RecursiveDelete" << std::endl; // TODO
    return {nullptr};
}

vector<BPlusNode> BPlusTree::SplitNode(BPlusNode node) {
    if(node.GetBytes() <= 4096 && branching_factor > node.pointer_map.size()) {
        return {node};
    }
    
    BPlusNode first(node.type), second(node.type);
    int first_size, second_size;

    if(node.type == BNodeType::LEAF) {
        int i = 0;
        for(auto key_val : node.value_map) {
            if(i < node.value_map.size() / 2) {
                first = first.InsertKV(key_val.first, key_val.second);
            }
            else {
                second = second.InsertKV(key_val.first, key_val.second);
            }
            i++;
        }
    }
    else {
        int i = 0;
        for(auto key_val : node.pointer_map) {
            if(i < node.pointer_map.size() / 2) {
                first = first.InsertKV(key_val.first, key_val.second);
            }
            else {
                second = second.InsertKV(key_val.first, key_val.second);
            }
            i++;
        }
    }

    return  {first, second};

}

void BPlusTree::Insert(vector<uint8_t> key, vector<uint8_t> value) {
    auto new_children = RecursiveInsert(manager.GetNode(root_pointer), key, value);
    if(new_children.size() == 1) {
        root_pointer = new_children[0].node_pointer;
    }
    else {
        BPlusNode new_root(BNodeType::NODE);
        vector<uint8_t> key;
        for(auto nc : new_children) {
            if(nc.type == BNodeType::LEAF) {
                key = nc.value_map.begin()->first;
            }
            else {
                key = nc.pointer_map.begin()->first;
            }
            new_root = new_root.InsertKV(key, nc.node_pointer);
        }
        root_pointer = manager.WriteNode(new_root);
    }
}

vector<BPlusNode> BPlusTree::RecursiveInsert(BPlusNode node, vector<uint8_t> key, vector<uint8_t> value) {
    if(node.type == BNodeType::LEAF) {
        if(node.value_map.size() == 0) {
            node = node.InsertKV(key, value);
            node.node_pointer = manager.WriteNode(node);
            return {node};
        }
        if(node.HasKey(key)) {
            node = node.UpdateKV(key, value);
        }
        else {
            node = node.InsertKV(key, value);
        }
        auto new_nodes = SplitNode(node);
        for(auto& n : new_nodes) {
            n.node_pointer = manager.WriteNode(n);
        }
        return new_nodes;
    }
    for(auto key_val = node.pointer_map.rbegin(); key_val != node.pointer_map.rend(); key_val++)
    {
        if(key_val->first <= key) {
            auto new_nodes = RecursiveInsert(manager.GetNode(key_val->second), key, value);
            node = node.UpdateKV(key_val->first, new_nodes[0].node_pointer);
            for(int j = 1; j < new_nodes.size(); j++) {
                if(new_nodes[j].type == BNodeType::LEAF) {
                    node = node.InsertKV(new_nodes[j].value_map.begin()->first, new_nodes[j].node_pointer);
                }
                else {
                    node = node.InsertKV(new_nodes[j].pointer_map.begin()->first, new_nodes[j].node_pointer);
                }
            }
            auto split_nodes = SplitNode(node);
            for(auto& n : split_nodes) {
                n.node_pointer = manager.WriteNode(n);
            }
            return split_nodes;
        }
    }
    std::cerr << "Shits fucked in RecursiveInsert" << std::endl; // TODO
    return {nullptr};
}

void BPlusTree::PrintTree() {
    PrintTreeRecursive(manager.GetNode(root_pointer));
}

void BPlusTree::PrintTreeRecursive(BPlusNode node) {
    if(node.type == BNodeType::LEAF) {
        node.PrintNodeData();
        return;
    }
    node.PrintNodeData();
    for(auto p : node.pointer_map) {
        PrintTreeRecursive(manager.GetNode(p.second));
    }
}