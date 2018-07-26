package com.netease.mail.holmes.BplusTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;


/**
 * Created by zuoqin on 2018/7/20.
 */
public class BPlusTree {

    private TreeNode root;
    private TreeNode aChild;
    private BPlusConfiguration conf;
    private LinkedList<Long> freeSlotPool;
    private LinkedList<Long> lookupPagesPool;
    private long firstPoolNextPointer;
    private long totalTreePages;
    private long maxPageNumber;
    private int deleteIterations;
    private BPlusTreePerformanceCounter bPerf = null;
    private FileMemroy mem;

    public BPlusTree(BPlusConfiguration conf, String mode,
                     String treeFilePath, BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        this.conf = conf;
        this.bPerf = bPerf;
        initializeCommon();
        this.mem=new FileMemroy(treeFilePath,conf);
        this.conf = conf;
        createTree();
        mem.updateMetaData(root,conf,false);
    }

    private void initializeCommon() {
        this.totalTreePages = 0L;
        this.maxPageNumber = 0L;
        this.deleteIterations = 0;
        this.firstPoolNextPointer = -1L;
        this.freeSlotPool = new LinkedList<>();
        this.lookupPagesPool = new LinkedList<>();
        this.bPerf.setBTree(this);
    }

    private TreeNode createTree() throws IOException, InvalidBTreeStateException {
        if(root == null) {
            root = new TreeLeaf(-1, -1,
                    TreeNodeType.TREE_ROOT_LEAF,
                    generateFirstAvailablePageIndex(conf));
            bPerf.incrementTotalPages();
            bPerf.incrementTotalLeaves();
            // write the file
            mem.setNode(root);

        }
        return(root);
    }

    public void insertKey(long key, String value, boolean unique)
            throws IOException, InvalidBTreeStateException,
            IllegalStateException, NumberFormatException {

        if(root == null)
        {throw new IllegalStateException("Can't insert to null tree");}

        if(key < 0)
        {throw new NumberFormatException("Can't have negative keys, sorry.");}

        value = conditionString(value);

        // check if our root is full
        if(root.isFull(conf)) {
            // allocate a new *internal* node, to be placed as the
            // *left* child of the new root
            aChild = this.root;
            TreeInternalNode node_buf = new TreeInternalNode(TreeNodeType.TREE_ROOT_INTERNAL,
                    generateFirstAvailablePageIndex(conf));
            node_buf.addPointerAt(0, aChild.getPageIndex());
            this.root = node_buf;

            // split root.
            splitTreeNode(node_buf, 0);
            mem.updateMetaData(root,this.conf,false);
            insertNonFull(node_buf, key, value, unique);
        }
        else
        {insertNonFull(root, key, value, unique);}
        bPerf.incrementTotalInsertions();
    }

    public RangeResult rangeSearch(long minKey, long maxKey, boolean unique)
            throws IOException, InvalidBTreeStateException {
        SearchResult sMin = searchKey(minKey, unique);
        SearchResult sMax;
        RangeResult rangeQueryResult = new RangeResult();
        if(sMin.isFound()) {
            // read up until we find a key that's greater than maxKey
            // or the last entry.

            int i = sMin.getIndex();
            while(sMin.getLeaf().getKeyAt(i) <= maxKey) {
                rangeQueryResult.getQueryResult().
                        add(new KeyValueWrapper(sMin.getLeaf().getKeyAt(i),
                                sMin.getLeaf().getValueAt(i)));

                // check if we have an overflow page
                if(!unique && sMin.getLeaf().getOverflowPointerAt(i) != -1)
                {parseOverflowPages(sMin.getLeaf(), i, rangeQueryResult);}

                i++;

                // check if we need to read the next block
                if(i == sMin.getLeaf().getCurrentCapacity()) {
                    // check if we have a next node to load.
                    if(sMin.getLeaf().getNextPagePointer() < 0)
                    // if not just break the loop
                    {break;}
                    sMin.setLeaf((TreeLeaf) mem.getNode(sMin.getLeaf().getNextPagePointer()));
                    i = 0;
                }
            }

        }
        // this is the case where both searches might fail to find something, but
        // we *might* have something between in the given range. To account for
        // that even if we have *not* found something we will return those results
        // instead. For example say we have a range of [2, 5] and we only have keys
        // from [3, 4], thus both searches for min and max would fail to find a
        // matching key in both cases. Thing is to account for that *both* results
        // will be stopped at the first key that is less than min and max values
        // given even if we did not find anything.
        else {
            sMax = searchKey(maxKey, unique);
            int i = sMax.getIndex();
            while(i >= 0 && sMax.getLeaf().getKeyAt(i) >= minKey) {
                rangeQueryResult.getQueryResult().
                        add(new KeyValueWrapper(sMax.getLeaf().getKeyAt(i),
                                sMax.getLeaf().getValueAt(i)));

                // check if we have an overflow page
                if(!unique && sMax.getLeaf().getOverflowPointerAt(i) != -1)
                {parseOverflowPages(sMax.getLeaf(), i, rangeQueryResult);}

                i--;
                // check if we need to read the next block
                if(i < 0) {
                    // check if we do have another node to load
                    if(sMax.getLeaf().getPrevPagePointer() < 0)
                    // if not just break the loop
                    {break;}
                    sMax.setLeaf((TreeLeaf)mem.getNode(sMax.getLeaf().getPrevPagePointer()));
                    // set it to max length
                    i = sMax.getLeaf().getCurrentCapacity()-1;
                }
            }

        }
        bPerf.incrementTotalRangeQueries();
        // finally return the result list (empty or not)
        return(rangeQueryResult);
    }
    private void parseOverflowPages(TreeLeaf l, int index, RangeResult res)
            throws IOException {
        TreeOverflow ovfPage = (TreeOverflow) mem.getNode(l.getOverflowPointerAt(index));
        int icap = 0;
        while(icap < ovfPage.getCurrentCapacity()) {
            res.getQueryResult().add(new KeyValueWrapper(l.getKeyAt(index),
                    ovfPage.getValueAt(icap)));
            icap++;
            // check if we have more pages
            if(icap == ovfPage.getCurrentCapacity() &&
                    ovfPage.getNextPagePointer() != -1L) {
                ovfPage = (TreeOverflow)mem.getNode(ovfPage.getNextPagePointer());
                icap = 0;
            }
        }
    }

    public SearchResult searchKey(long key, boolean unique)
            throws IOException, InvalidBTreeStateException {
        bPerf.incrementTotalSearches();
        return(searchKey(this.root, key, unique));
    }

    private SearchResult searchKey(TreeNode node, long key, boolean unique)
            throws IOException {
        // search for the key
        int i = binSearchBlock(node, key, Rank.Exact);

        // check if we found it
        if(node.isLeaf()) {
            //i--;
            if(i >= 0 && i < node.getCurrentCapacity() && key == node.getKeyAt(i)) {

                // we found the key, depending on the unique flag handle accordingly
                if(unique || ((TreeLeaf)node).getOverflowPointerAt(i) == -1L )
                {return(new SearchResult((TreeLeaf)node, i, true));}
                // handle the case of duplicates where actual overflow pages exist
                else {
                    TreeLeaf lbuf = (TreeLeaf)node;
                    TreeOverflow ovfBuf = (TreeOverflow)mem.getNode(lbuf.getOverflowPointerAt(i));
                    LinkedList<String> ovfList = new LinkedList<>();
                    // add the current one
                    ovfList.add(lbuf.getValueAt(i));
                    int icap = 0;
                    // loop through all the overflow pages
                    while(icap < ovfBuf.getCurrentCapacity()) {
                        ovfList.add(ovfBuf.getValueAt(icap));
                        icap++;
                        // advance if we have another page
                        if(icap == ovfBuf.getCurrentCapacity() &&
                                ovfBuf.getNextPagePointer() != -1L) {
                            ovfBuf = (TreeOverflow)mem.getNode(ovfBuf.getNextPagePointer());
                            icap = 0;
                        }
                    }
                    // now after populating the list return the search result
                    return(new SearchResult((TreeLeaf)node, i, ovfList));
                }
            }
            else
            // we found nothing, use the unique constructor anyway.
            {return(new SearchResult((TreeLeaf)node, i, false));}

        }
        // probably it's an internal node, descend to a leaf
        else {
            // padding to account for the last pointer (if needed)
            if(i != node.getCurrentCapacity() && key >= node.getKeyAt(i)) {i++;}
            TreeNode t = mem.getNode(((TreeInternalNode)node).getPointerAt(i));
            return(searchKey(t, key, unique));
        }

    }



    /*public DeleteResult deleteKey(long key, boolean unique)
            throws IOException, InvalidBTreeStateException  {
        if(root.isEmpty()) {
            return (new DeleteResult(key, (LinkedList<String>) null));
        } else
        {return(deleteKey(root, null, -1, -1, key, unique));}
    }

    public DeleteResult deleteKey(TreeNode current, TreeInternalNode parent,
                                  int parentPointerIndex, int parentKeyIndex,
                                  long key, boolean unique)
            throws IOException, InvalidBTreeStateException {

        // check if we need to consolidate
        if(current.isTimeToMerge(conf)) {
            //System.out.println("Parent needs merging (internal node)");
            TreeNode mres = mergeOrRedistributeTreeNodes(current, parent,
                    parentPointerIndex, parentKeyIndex);
            if(mres != null) {
                current = mres;
            }
        }

        LinkedList<String> rvals;
        // search for the key
        int i = binSearchBlock(current, key, Rank.Succ);
        // check if it's an internal node
        if(current.isInternalNode()) {
            // if it is, descend to a leaf
            TreeInternalNode inode = (TreeInternalNode)current;
            int idx = i;
            // check if we are at the end
            if(key >= current.getKeyAt(i)) {
                idx++;
            }
            // read the next node
            TreeNode next = readNode(inode.getPointerAt(idx));
            // finally return the resulting set
            return(deleteKey(next, inode, idx, i*//*keyLocation*//*, key, unique));
        }
        // the current node, is a leaf.
        else if(current.isLeaf()) {
            TreeLeaf l = (TreeLeaf)current;
            // check if we actually found the key
            if(i == l.getCurrentCapacity()) {
                //System.out.println("Key with value: " + key +
                //        " not found, reached limits");
                return (new DeleteResult(key, (LinkedList<String>) null));
            } else if(key != l.getKeyAt(i)) {
                //System.out.println("Key with value: " + key + " not found, key mismatch");
                //throw new InvalidBTreeStateException("Key not found!");
                return (new DeleteResult(key, (LinkedList<String>) null));
            }
            else {

                //System.out.println("Found the key " + key + ", removing it");
                rvals = new LinkedList<>();

                // we we *have* to make a choice on where to make
                // a read trade off.

                // check if we have an overflow page
                if(l.getOverflowPointerAt(i) != -1) {
                    TreeOverflow ovf = null;
                    TreeOverflow povf =
                            (TreeOverflow)readNode(l.getOverflowPointerAt(i));

                    // handle singular deletes
                    if(unique) {

                        // descend to the last page
                        while(povf.getNextPagePointer() != -1L) {
                            ovf = povf;
                            povf = (TreeOverflow)readNode(povf.getNextPagePointer());
                        }

                        // remove from the overflow page the value
                        rvals.add(povf.removeLastValue());
                        povf.decrementCapacity(conf);

                        // if the page is empty, delete it.
                        if(povf.isEmpty()) {
                            if (ovf == null) {
                                l.setOverflowPointerAt(i, -1L);
                                l.writeNode(treeFile, conf, bPerf);
                            }
                            else {
                                ovf.setNextPagePointer(-1L);
                                ovf.writeNode(treeFile, conf, bPerf);
                            }
                            // now delete the page
                            deletePage(povf.getPageIndex(), false);
                        }
                        // we don't have to delete the page, so let's
                        // update it instead.
                        else
                        {povf.writeNode(treeFile, conf, bPerf);}

                        // return the result
                        return(new DeleteResult(key, rvals));
                    }
                    // we have to delete all the values
                    else {

                        // here to save reads/writes we just
                        // "delete-as-we-read"
                        while(povf.getCurrentCapacity() > 0) {
                            rvals.add(povf.removeLastValue());
                            povf.decrementCapacity(conf);

                            // check if it's time to remove the page
                            if(povf.isEmpty()) {
                                deletePage(povf.getPageIndex(), false);
                                if(povf.getNextPagePointer() != -1L)
                                {povf = (TreeOverflow)readNode(povf.getNextPagePointer());}
                            }
                        }
                    }

                }
            }
            // we reached here because either we have no overflow page
            // or non-unique deletes with overflow pages. We should
            // reach this point after we purged all the overflow pages.
            rvals.add(((TreeLeaf)current).removeEntryAt(i, conf));
            current.writeNode(treeFile, conf, bPerf);
        }
        else {
            throw new IllegalStateException("Read unknown or " +
                    "overflow page while descending");
        }

        return(new DeleteResult(key, rvals));
    }*/


    private void insertNonFull(TreeNode n, long key, String value, boolean unique)
            throws IOException, InvalidBTreeStateException {
        boolean useChild = true;
        int i = binSearchBlock(n, key, Rank.PlusOne);
        // check if we have a leaf
        if(n.isLeaf()) {
            TreeLeaf l = (TreeLeaf)n;

            // before we add it, let's check if the key already exists
            // and if it does pull up (or create) the overflow page and
            // add the value there.
            //
            // Not that we do *not* add the key if we have a true unique flag


            // this is to adjust for a corner case due to indexing
            int iadj = (n.getCurrentCapacity() > 0 &&
                    i == 0 && n.getFirstKey() > key) ? i : i-1;
            if(n.getCurrentCapacity() > 0 && n.getKeyAt(iadj) == key) {

                if(unique) {
                    //System.out.println("Duplicate entry found and unique " +
                    //        "flag enabled, can't add");
                    return;
                }

                //System.out.println("Duplicate found! Adding to overflow page!");

                // overflow page does not exist, yet; time to create it!
                if(l.getOverflowPointerAt(iadj) < 0) {
                    createOverflowPage(l, iadj, value);
                }
                // page already exists, so pull it and check if it has
                // available space, if it does all is good; otherwise we
                // pull the next overflow page or we create another one.
                else {

                    TreeOverflow ovf =
                            (TreeOverflow) mem.getNode(l.getOverflowPointerAt(iadj));

                    while(ovf.isFull(conf)) {
                        // check if we have more, if not create
                        if(ovf.getNextPagePointer() < 0)
                        // create page and return
                        {createOverflowPage(ovf, -1, value); return;}
                        // load the next page
                        else
                        {ovf = (TreeOverflow) mem.getNode(ovf.getNextPagePointer());}
                    }

                    // if the loaded page is not full then add it.
                    ovf.pushToValueList(value);
                    ovf.incrementCapacity(conf);
                    //ovf.writeNode(treeFile, conf, bPerf);
                    mem.setNode(ovf);
                }
            }

            // we have a new key insert
            else {
                // now add the (Key, Value) pair
                l.addToValueList(i, value);
                l.addToKeyArrayAt(i, key);
                // also create a NULL overflow pointer
                l.addToOverflowList(i, -1L);
                l.incrementCapacity(conf);
                // commit the changes
                //l.writeNode(treeFile, conf, bPerf);
                mem.setNode(l);
            }

        } else {

            // This requires a bit of explanation; the above while loop
            // starts initially from the *end* key parsing the *right*
            // child, so initially it's like this:
            //
            //  Step 0:
            //
            //  Key Array:          | - | - | - | x |
            //  Pointer Array:      | - | - | - | - | x |
            //
            // Now if the while loop stops there, we have a *right* child
            // pointer, but should it continues we get the following:
            //
            // Step 1:
            //
            //  Key Array:          | - | - | x | - |
            //  Pointer Array:      | - | - | - | x | - |
            //
            //  and finally we reach the special case where we have the
            //  following:
            //
            // Final step:
            //
            //  Key Array:          | x | - | - | - |
            //  Pointer Array:      | x | - | - | - | - |
            //
            //
            // In this case we have a *left* pointer, which can be
            // quite confusing initially... hence the changed naming.
            //
            //

            TreeInternalNode inode = (TreeInternalNode)n;
            aChild = mem.getNode(inode.getPointerAt(i));
            if (aChild.isOverflow() || aChild.isLookupPageOverflowNode()) {
                throw new InvalidBTreeStateException("aChild can't be overflow node");
            }
            TreeNode nextAfterAChild = null;
            if(aChild.isFull(conf)) {
                splitTreeNode(inode, i);
                if (key >= n.getKeyAt(i)) {
                    useChild = false;
                    nextAfterAChild = mem.getNode(inode.getPointerAt(i+1));
                }
            }

            insertNonFull(useChild ? aChild : nextAfterAChild, key, value, unique);
        }
    }
    private void splitTreeNode(TreeInternalNode n, int index)
            throws IOException, InvalidBTreeStateException {

//        System.out.println("-- Splitting node with index: " +
//                aChild.getPageIndex() + " of type: " +
//                aChild.getNodeType().toString());

        int setIndex;
        TreeNode znode;
        long keyToAdd;
        TreeNode ynode = aChild; // x.c_{i}
        if(ynode.isInternalNode()) {
            TreeInternalNode zInternal,
                    yInternal = (TreeInternalNode) ynode;

            zInternal = new TreeInternalNode(TreeNodeType.TREE_INTERNAL_NODE,
                    generateFirstAvailablePageIndex(conf));

            bPerf.incrementTotalInternalNodes();


            setIndex =  conf.getTreeDegree()-1;

            int i;
            for(i = 0; i < setIndex; i++) {
                zInternal.addToKeyArrayAt(i, yInternal.popKey());
                zInternal.addPointerAt(i, yInternal.popPointer());
            }
            zInternal.addPointerAt(i, yInternal.popPointer());
            //keyToAdd = ynode.getFirstKey();
            keyToAdd = ynode.popKey();

            zInternal.setCurrentCapacity(setIndex);
            yInternal.setCurrentCapacity(setIndex);

            // it it was the root, invalidate it and make it a regular internal node
            if(yInternal.isRoot()) {
                yInternal.setNodeType(TreeNodeType.TREE_INTERNAL_NODE);
                bPerf.incrementRootSplits();
            }
            bPerf.incrementInternalNodeSplits();

            // update pointer at n_{index+1}
            n.addPointerAt(index, zInternal.getPageIndex());
            // update key value at n[index]
            n.addToKeyArrayAt(index, keyToAdd);
            // adjust capacity
            n.incrementCapacity(conf);
            // update shared child pointer.
            aChild = zInternal;
            // update reference
            znode = zInternal;
        }
        // we have a leaf
        else {
            TreeLeaf zLeaf,
                    yLeaf = (TreeLeaf) ynode,
                    afterLeaf;

            zLeaf = new TreeLeaf(yLeaf.getNextPagePointer(),
                    yLeaf.getPageIndex(), TreeNodeType.TREE_LEAF,
                    generateFirstAvailablePageIndex(conf));

            // update the previous pointer from the node after ynode
            if(yLeaf.getNextPagePointer() != -1) {
                afterLeaf = (TreeLeaf) mem.getNode(yLeaf.getNextPagePointer());
                afterLeaf.setPrevPagePointer(zLeaf.getPageIndex());
                //afterLeaf.writeNode(treeFile, conf, bPerf);
                mem.setNode(afterLeaf);
            }

            bPerf.incrementTotalLeaves();

            // update pointers in ynode, only have to update next pointer
            yLeaf.setNextPagePointer(zLeaf.getPageIndex());

            setIndex = conf.getLeafNodeDegree()-1;

            for(int i = 0; i < setIndex; i++) {
                //long fk = ynode.getLastKey();
                //long ovf1 = ((TreeLeaf)ynode).getLastOverflowPointer();
                zLeaf.pushToKeyArray(yLeaf.removeLastKey());
                zLeaf.pushToValueList(yLeaf.removeLastValue());
                zLeaf.pushToOverflowList(yLeaf.removeLastOverflowPointer());
                zLeaf.incrementCapacity(conf);
                yLeaf.decrementCapacity(conf);
            }

            // it it was the root, invalidate it and make it a regular leaf
            if(yLeaf.isRoot()) {
                yLeaf.setNodeType(TreeNodeType.TREE_LEAF);
                bPerf.incrementRootSplits();
            }
            bPerf.incrementTotalLeafSplits();

            // update pointer at n_{index+1}
            n.addPointerAt(index + 1, zLeaf.getPageIndex());
            // update key value at n[index]
            n.addToKeyArrayAt(index, zLeaf.getKeyAt(0));
            // adjust capacity
            n.incrementCapacity(conf);
            // update reference
            znode = zLeaf;
        }

        znode.setBeingDeleted(false);
        // commit the changes
        //znode.writeNode(treeFile, conf, bPerf);
        mem.setNode(znode);
        mem.setNode(ynode);
        //ynode.writeNode(treeFile, conf, bPerf);
        //n.writeNode(treeFile, conf, bPerf);
        mem.setNode(n);
        // commit page counts
        //updatePageIndexCounts(conf);
    }


    private String conditionString(String s) {
        if(s == null) {
            s = " ";
            //System.out.println("Cannot have a null string");
        }

        if(s.length() > conf.getEntrySize()) {
            System.out.println("Satellite length can't exceed " +
                    conf.getEntrySize() + " trimming...");
            s = s.substring(0, conf.getEntrySize());
        } else if(s.length() < conf.getEntrySize()) {
            //System.out.println("Satellite length can't be less than" +
            //        conf.getEntrySize() + ", adding whitespaces to make up");
            int add = conf.getEntrySize() - s.length();
            for(int i = 0; i < add; i++) {s = s + " ";}
        }
        return(s);
    }

    private long generateFirstAvailablePageIndex(BPlusConfiguration conf) {
        long index;
        // check if we have used pages
        if(freeSlotPool.size() > 0)
        {index = freeSlotPool.pop(); totalTreePages++; return(index);}
        // if not pad to the end of the file.
        else {
            if (maxPageNumber <= totalTreePages) {
                maxPageNumber++;
            }
            totalTreePages++;
            index = conf.getPageSize() * (maxPageNumber + 1);
            return(index);
        }
    }
    private int binSearchBlock(TreeNode n, long key, Rank rank) {
        return binSearchRec(n, 0, n.getCurrentCapacity() - 1, key, rank);
    }

    /**
     * Binary search implementation for tree blocks.
     *
     * @param n    node to search
     * @param l    left (lower-part) array index
     * @param r    right (upper-part) array index
     * @param key  key to search
     * @param rank rank of the search (for lower/upper bound)
     * @return the index of the bound or found key.
     */
    private int binSearchRec(TreeNode n, int l, int r, long key, Rank rank) {
        int m;
        long mkey;

        if (l > r) {
            switch (rank) {
                case Pred:
                    return l == 0 ? l : l - 1;
                case Succ:
                    return l > 0 && l == n.getCurrentCapacity() ? l - 1 : l;
                //case Exact: return l;
                default:
                    return l;
            }
        } else {
            m = (l + r) / 2;
            mkey = n.getKeyAt(m);
        }

        if (mkey < key) {
            return binSearchRec(n, m + 1, r, key, rank);
        } else if (mkey > key) {
            return binSearchRec(n, l, m - 1, key, rank);
        } else { // this is equal
            return Rank.PlusOne == rank ? m + 1 : m;
        }
    }
    private void createOverflowPage(TreeNode n, int index, String value)
            throws IOException, InvalidBTreeStateException {
        TreeOverflow novf;
        if(n.isOverflow()) {
            TreeOverflow ovf = (TreeOverflow)n;
            novf = new TreeOverflow(-1L, ovf.getPageIndex(),
                    generateFirstAvailablePageIndex(conf));
            // push the first value
            novf.pushToValueList(value);
            novf.incrementCapacity(conf);
            // update overflow pointer to parent node
            ovf.setNextPagePointer(novf.getPageIndex());
            // set being deleted to false
            novf.setBeingDeleted(false);
            // commit changes to new overflow page
            //novf.writeNode(treeFile, conf, bPerf);
            mem.setNode(novf);
            // commit changes to old overflow page
            //ovf.writeNode(treeFile, conf, bPerf);
            mem.setNode(ovf);
        } else if(n.isLeaf()) {
            TreeLeaf l = (TreeLeaf)n;
            novf = new TreeOverflow(-1L, l.getPageIndex(),
                    generateFirstAvailablePageIndex(conf));
//            System.out.println("Creating overflow page with index: " + novf.getPageIndex() +
//                    " for key: " + l.getKeyAt(index));
            // push the first value
            novf.pushToValueList(value);
            novf.incrementCapacity(conf);
            // update overflow pointer to parent node
            l.setOverflowPointerAt(index, novf.getPageIndex());
            // set being deleted to false
            novf.setBeingDeleted(false);
            // commit changes to overflow page
            mem.setNode(novf);
            //novf.writeNode(treeFile, conf, bPerf);
            // commit changes to leaf page
            //l.writeNode(treeFile, conf, bPerf);
            mem.setNode(l);
            // commit page counts
        } else {
            throw new InvalidBTreeStateException("Expected Leaf or Overflow, " +
                    "got instead: " + n.getNodeType().toString());
        }

        bPerf.incrementTotalOverflowPages();
        // commit page counts
        //updatePageIndexCounts(conf);
    }
    /*private TreeNode readNode(long index) throws IOException {

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
            bPerf.incrementTotalInternalNodeReads();
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
            bPerf.incrementTotalOverflowReads();
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
            bPerf.incrementTotalLeafNodeReads();

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
    }*/

    public void close() throws IOException{
        this.mem.writeToFile(conf,bPerf);
        this.mem.updateMetaData(this.root,conf,true);

    }

    private enum Rank {Pred, Succ, PlusOne, Exact}
}
