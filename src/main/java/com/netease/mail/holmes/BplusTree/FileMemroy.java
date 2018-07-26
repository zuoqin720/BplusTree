package com.netease.mail.holmes.BplusTree;

import java.io.IOException;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by zuoqin on 2018/7/20.
 */
public class FileMemroy {

    private TreeMap<Long,TreeNode> treeMap;
    private TreeFile treeFile;
    private BPlusConfiguration conf;


    public FileMemroy(String path ,BPlusConfiguration bPlusConfiguration) throws IOException{
        this.treeMap = new TreeMap<Long, TreeNode>();
        this.treeFile = new TreeRandomAccessFile(path);
        this.conf = bPlusConfiguration;
    }

    public void setNode(TreeNode node){

        treeMap.put(node.getPageIndex(),node);
    }

    public TreeNode getNode(long index)throws IOException{
        if (treeMap.get(index)!=null){
            return treeMap.get(index);
        }
        return readNode(index);
    }

    private TreeNode readNode(long index) throws IOException {

        // caution.
        if(index < 0)
        {return(null);}
        treeFile.seek(index);
        // get the page type
        TreeNodeType nt = getPageType(treeFile.readShort());

        // handle internal node reading
        if(isInternalNode(nt)) {
            TreeInternalNode tnode = new TreeInternalNode(nt, index);
            int curCap = treeFile.readInt();
            for(int i = 0; i < curCap; i++) {
                tnode.addToKeyArrayAt(i, treeFile.readLong());
                tnode.addPointerAt(i, treeFile.readLong());
            }
            // add the final pointer
            tnode.addPointerAt(curCap, treeFile.readLong());
            // update the capacity
            tnode.setCurrentCapacity(curCap);
            //bPerf.incrementTotalInternalNodeReads();
            // set deleted flag
            tnode.setBeingDeleted(false);
            return(tnode);
        }
        // check if we have an overflow page
        else if(isOverflowPage(nt)) {
            long nextptr = treeFile.readLong();
            long prevptr = treeFile.readLong();
            int curCap = treeFile.readInt();
            byte[] strBuf = new byte[conf.getEntrySize()];
            TreeOverflow tnode = new TreeOverflow(nextptr, prevptr, index);

            // read entries
            for(int i = 0; i < curCap; i++) {
                treeFile.read(strBuf);
                tnode.addToValueList(i, new String(strBuf));
            }
            // update capacity
            tnode.setCurrentCapacity(curCap);
            //bPerf.incrementTotalOverflowReads();
            return(tnode);
        }
        // well, it must be a leaf node
        else if (isLeaf(nt)) {
            long nextptr = treeFile.readLong();
            long prevptr = treeFile.readLong();
            int curCap = treeFile.readInt();
            byte[] strBuf = new byte[conf.getEntrySize()];
            TreeLeaf tnode = new TreeLeaf(nextptr, prevptr, nt, index);

            // read entries
            for(int i = 0; i < curCap; i++) {
                tnode.addToKeyArrayAt(i, treeFile.readLong());
                tnode.addToOverflowList(i, treeFile.readLong());
                treeFile.read(strBuf);
                tnode.addToValueList(i, new String(strBuf));
            }
            // update capacity
            tnode.setCurrentCapacity(curCap);
            //bPerf.incrementTotalLeafNodeReads();

            // set being deleted to false
            tnode.setBeingDeleted(false);

            return(tnode);
        } else {
            long nextptr = treeFile.readLong();
            int curCap = treeFile.readInt();
            TreeLookupOverflowNode lpOvf = new TreeLookupOverflowNode(index, nextptr);

            // now loop through the
            for (int i = 0; i < curCap; i++) {
                lpOvf.addToKeyArrayAt(i, treeFile.readLong());
            }

            // update capacity
            lpOvf.setCurrentCapacity(curCap);

            return (lpOvf);
        }
    }

    private TreeNodeType getPageType(short pval)
            throws InvalidPropertiesFormatException {
        switch(pval) {

            case 1:         // LEAF
            {return(TreeNodeType.TREE_LEAF);}

            case 2:         // INTERNAL NODE
            {return(TreeNodeType.TREE_INTERNAL_NODE);}

            case 3:         // INTERNAL NODE /w ROOT
            {return(TreeNodeType.TREE_ROOT_INTERNAL);}

            case 4:         // LEAF NODE /w ROOT
            {return(TreeNodeType.TREE_ROOT_LEAF);}

            case 5:         // LEAF OVERFLOW NODE
            {return(TreeNodeType.TREE_LEAF_OVERFLOW);}

            case 6:         // TREE_LOOKUP_OVERFLOW
            {
                return (TreeNodeType.TREE_LOOKUP_OVERFLOW);
            }
            default: {
                throw new InvalidPropertiesFormatException("Unknown " +
                        "node value read; file possibly corrupt?");
            }
        }
    }
    private boolean isInternalNode(TreeNodeType nt) {
        return(nt == TreeNodeType.TREE_INTERNAL_NODE ||
                nt == TreeNodeType.TREE_ROOT_INTERNAL);
    }

    /**
     * Check if the node is an overflow page
     *
     * @param nt nodeType of the node we want to check
     * @return return true if it's an overflow page, false if it's not.
     */
    public boolean isOverflowPage(TreeNodeType nt)
    {return(nt == TreeNodeType.TREE_LEAF_OVERFLOW);}

    /**
     * Check if the node is a leaf page
     *
     * @param nt nodeType of the node we want to check
     * @return return true it's a leaf page, false if it's not
     */
    public boolean isLeaf(TreeNodeType nt) {
        return(nt == TreeNodeType.TREE_LEAF ||
                nt == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nt == TreeNodeType.TREE_ROOT_LEAF);
    }


    public void updateMetaData(TreeNode treeNode, BPlusConfiguration bconf, boolean sync) throws IOException{
        treeFile.seek(0);
        treeFile.writeLong(treeNode.getPageIndex());
        treeFile.writeLong(bconf.getPageSize());
        treeFile.writeLong(bconf.getKeySize());
        treeFile.writeLong(bconf.getEntrySize());
    }

    public void writeToFile(BPlusConfiguration bconf,BPlusTreePerformanceCounter bPerf)throws IOException {
        Iterator iter=treeMap.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry) iter.next();
            String key = entry.getKey().toString();
            TreeNode treeNode = (TreeNode) entry.getValue();
            treeNode.writeNode(this.treeFile,bconf,bPerf);
        }
    }

}
