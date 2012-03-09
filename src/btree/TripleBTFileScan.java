/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package btree;
import java.io.*;
import global.*;
import heap.*;

/**
 * TripleBTFileScan implements a search/iterate interface to label B+ tree 
 * index files (class TripleBTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class TripleBTFileScan  extends IndexFileScan
             implements  GlobalConst
{

  TripleBTreeFile bfile; 
  String treeFilename;     // B+ tree we're scanning 
  TripleBTLeafPage leafPage;   // leaf page containing current record
  TID curTid;       // position in current leaf; note: this is 
                             // the RID of the key/RID pair within the
                             // leaf page.                                    
  boolean didfirst;        // false only before getNext is called
  boolean deletedcurrent;  // true after deleteCurrent is called (read
                           // by get_next, written by deleteCurrent).
    
  KeyClass endkey;    // if NULL, then go all the way right
                        // else, stop when current record > this value.
                        // (that is, implement an inclusive range 
                        // scan -- the only way to do a search for 
                        // a single value).
  int keyType;
  int maxKeysize;

  /**
   * Iterate once (during a scan).  
   *@return null if done; otherwise next KeyDataEntry
   *@exception ScanIteratorException iterator error
   */
  public KeyDataEntry get_next() 
    throws ScanIteratorException
    {

    KeyDataEntry entry;
    PageId nextpage;
    try {
      if (leafPage == null)
        return null;
      
      if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
         didfirst = true;
         deletedcurrent = false;
         entry=leafPage.getCurrent(curTid);
      }
      else {
         entry = leafPage.getNext(curTid);
      }

      while ( entry == null ) {
         nextpage = leafPage.getNextPage();
         SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
	 if (nextpage.pid == INVALID_PAGE) {
	    leafPage = null;
	    return null;
	 }

         leafPage=new TripleBTLeafPage(nextpage, keyType);
	 	
	 entry=leafPage.getFirst(curTid);
      }

      if (endkey != null)  
        if ( TripleBT.keyCompare(entry.key, endkey)  > 0) {
            // went past right end of scan 
	    SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
            leafPage=null;
	    return null;
        }

      return entry;
    }
    catch ( Exception e) {
         e.printStackTrace();
         throw new ScanIteratorException();
    }
  }


  /**
   * Delete currently-being-scanned(i.e., just scanned)
   * data entry.
   *@exception ScanDeleteException  delete error when scan
   */
  public void delete_current() 
    throws ScanDeleteException {

    KeyDataEntry entry;
    try{  
      if (leafPage == null) {
	System.out.println("No Record to delete!"); 
	throw new ScanDeleteException();
      }
      
      if( (deletedcurrent == true) || (didfirst==false) ) 
	return;    
      
      entry=leafPage.getCurrent(curTid);  
      SystemDefs.JavabaseBM.unpinPage( leafPage.getCurPage(), false);
      bfile.Delete(entry.key, ((TripleLeafData)entry.data).getData());
      leafPage=bfile.findRunStart(entry.key, curTid);
      
      deletedcurrent = true;
      return;
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ScanDeleteException();
    }  
  }
  
  /** max size of the key
   *@return the maxumum size of the key in BTFile
   */
  public int keysize() {
    return maxKeysize;
  }  
  
  
  
  /**
  * destructor.
  * unpin some pages if they are not unpinned already.
  * and do some clearing work.
  *@exception IOException  error from the lower layer
  *@exception bufmgr.InvalidFrameNumberException  error from the lower layer
  *@exception bufmgr.ReplacerException  error from the lower layer
  *@exception bufmgr.PageUnpinnedException  error from the lower layer
  *@exception bufmgr.HashEntryNotFoundException   error from the lower layer
  */
  public  void DestroyBTreeFileScan()
    throws  IOException, bufmgr.InvalidFrameNumberException,bufmgr.ReplacerException,
            bufmgr.PageUnpinnedException,bufmgr.HashEntryNotFoundException   
  { 
     if (leafPage != null) {
         SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
     } 
     leafPage=null;
  }




}





