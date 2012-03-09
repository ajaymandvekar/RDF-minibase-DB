/*
 * @(#) SortedPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *
 *      by Xiaohu Li (xiaohu@cs.wisc.edu)
 */

//package labelbtree;
package btree;

import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import heap.*;
//import btree.*;
import labelheap.*;


/**
 * LabelBTsortedPage class 
 * just holds abstract records in sorted order, based 
 * on how they compare using the key interface from LabelBT.java.
 */
public class LabelBTSortedPage  extends LHFPage{

  
  int keyType; //it will be initialized in BTFile
  
  
  /** pin the page with pageno, and get the corresponding SortedPage
   *@param pageno input parameter. To specify which page number the
   *  BTSortedPage will correspond to.
   *@param keyType input parameter. It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger. 
   *@exception  ConstructPageException  error for LabelBTSortedPage constructor
   */
  public LabelBTSortedPage(PageId pageno, int keyType) 
    throws ConstructPageException 
    { 
      super();
      try {
	// super();
	SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/); 
	this.keyType=keyType;   
      }
      catch (Exception e) {
	throw new ConstructPageException(e, "construct sorted page failed");
      }
    }
  
  /**associate the SortedPage instance with the Page instance 
   *@param page input parameter. To specify which page  the
   *  BTSortedPage will correspond to.
   *@param keyType input parameter. It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger. 
   */
  public LabelBTSortedPage(Page page, int keyType) {
    
    super(page);
    this.keyType=keyType;   
  }  
  
  
  /**new a page, and associate the SortedPage instance with the Page instance
   *@param keyType input parameter. It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger. 
   *@exception  ConstructPageException error for BTSortedPage constructor
   */ 
  public LabelBTSortedPage(int keyType) 
    throws ConstructPageException
    {
      super();
      try{
	Page apage=new Page();
	PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
	if (pageId==null) 
	  throw new ConstructPageException(null, "construct new page failed");
	this.init(pageId, apage);
	this.keyType=keyType;   
      }
      catch (Exception e) {
        e.printStackTrace();
	throw new ConstructPageException(e, "construct sorted page failed");
      }
    }  
  
  /**
   * Performs a sorted insertion of a record on an record page. The records are
   *  sorted in increasing key order.
   *  Only the  slot  directory is  rearranged.  The  data records remain in
   *  the same positions on the  page.
   * 
   *@param entry the entry to be inserted. Input parameter.
   *@return its lid where the entry was inserted; null if no space left.
   *@exception  InsertRecException error when insert
   */
   protected LID insertRecord( KeyDataEntry entry)
          throws InsertRecException 
   {
     int i;
     short  nType;
     LID lid;
     byte[] record;
     // ASSERTIONS:
     // - the slot directory is compressed; Inserts will occur at the end
     // - slotCnt gives the number of slots used
     
     // general plan:
     //    1. Insert the record into the page,
     //       which is then not necessarily any more sorted
     //    2. Sort the page by rearranging the slots (insertion sort)
     
     try {
       
       record= LabelBT.getBytesFromEntry(entry);  
       lid=super.insertRecord(record);
         if (lid==null) return null;
	 
         if ( entry.data instanceof LabelLeafData )
	   nType= NodeType.LEAF;
         else  //  entry.data instanceof IndexData              
	   nType= NodeType.INDEX;
	 
	 
	 // performs a simple insertion sort
	 for (i=getSlotCnt()-1; i > 0; i--) 
	   {
	     
	     KeyClass key_i, key_iplus1;
	     
	     key_i=LabelBT.getEntryFromBytes(getpage(), getSlotOffset(i), 
					getSlotLength(i), keyType, nType).key;
	     
	     key_iplus1=LabelBT.getEntryFromBytes(getpage(), getSlotOffset(i-1), 
					     getSlotLength(i-1), keyType, nType).key;
	     
	     if (LabelBT.keyCompare(key_i, key_iplus1) < 0)
	       {
	       // switch slots:
		 int ln, off;
		 ln= getSlotLength(i);
		 off=getSlotOffset(i);
		 setSlot(i,getSlotLength(i-1),getSlotOffset(i-1));  
		 setSlot(i-1, ln, off);
	       } else {
		 // end insertion sort
		 break;
	       }
	     
	   }
	 
	 // ASSERTIONS:
	 // - record keys increase with increasing slot number 
	 // (starting at slot 0)
	 // - slot directory compacted
	 
	 lid.slotNo = i;
	 return lid;
     }
     catch (Exception e ) { 
       throw new InsertRecException(e, "insert record failed"); 
     }
     
     
   } // end of insertRecord
 

  /**  Deletes a record from a sorted record page. It also calls
   *    HFPage.compact_slot_dir() to compact the slot directory.
   *@param lid it specifies where a record will be deleted
   *@return true if success; false if lid is invalid(no record in the lid).
   *@exception DeleteRecException error when delete
   */
  public  boolean deleteSortedRecord(LID lid)
    throws DeleteRecException
    {
      try {
	
	deleteRecord(lid);
	compact_slot_dir();
	return true;  
	// ASSERTIONS:
	// - slot directory is compacted
      }
      catch (Exception  e) {
	if (e instanceof heap.InvalidSlotNumberException)
	  return false;
	else
	  throw new DeleteRecException(e, "delete record failed");
      }
    } // end of deleteSortedRecord
  
  /** How many records are in the page
   *@param return the number of records.
   *@exception IOException I/O errors
   */
  protected int numberOfRecords() 
    throws IOException
    {
      return getSlotCnt();
    }
};




