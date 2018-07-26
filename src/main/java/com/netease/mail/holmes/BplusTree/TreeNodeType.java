package com.netease.mail.holmes.BplusTree;

/**
 * Created by zuoqin on 2018/7/17.
 */
enum TreeNodeType {
    TREE_LEAF,
    TREE_INTERNAL_NODE,
    TREE_ROOT_INTERNAL,
    TREE_ROOT_LEAF,
    TREE_LEAF_OVERFLOW,
    TREE_LOOKUP_OVERFLOW
}