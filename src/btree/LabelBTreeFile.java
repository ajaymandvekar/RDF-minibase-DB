/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

/** labelbtfile.java
 * This is the main definition of class LabelBTreeFile, which derives from 
 * abstract base class LabelIndexFile.
 * It provides an insert/delete interface.
 */
public class LabelBTreeFile extends LabelIndexFile 
  implements GlobalConst {
  
  private final static int MAGIC0=1989;
  
  private final static String lineSep=System.getProperty("line.separator");
  
  private static FileOutputStream fos;
  private static DataOutputStream trace;
  
  
  /** It causes a structured trace to be written to a
   * file.  This output is
   * used to drive a visualization tool that shows the inner workings of the
   * b-tree during its operations. 
   *@param filename input parameter. The trace file name
   *@exception IOException error from the lower layer
   */ 
  public static void traceFilename(String filename) 
    throws  IOException
    {
      
      fos=new FileOutputStream(filename);
      trace=new DataOutputStream(fos);
    }
  
  /** Stop tracing. And close trace file. 
   *@exception IOException error from the lower layer
   */
  public static void destroyTrace() 
    throws  IOException
    {
      if( trace != null) trace.close();
      if( fos != null ) fos.close();
      fos=null;
      trace=null;
    }
  
  
  private LabelBTreeHeaderPage headerPage;
  private  PageId  headerPageId;
  private String  dbname;  
  
  /**
   * Access method to data member.
   * @return  Return a BTreeHeaderPage object that is the header page
   *          of this btree file.
   */
  public LabelBTreeHeaderPage getHeaderPage() {
    return headerPage;
  }
  
  private PageId get_file_entry(String filename)         
    throws GetFileEntryException
    {
      try {
	return SystemDefs.JavabaseDB.get_file_entry(filename);
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new GetFileEntryException(e,"");
      }
    }
  
  
  
  private Page pinPage(PageId pageno) 
    throws PinPageException
    {
      try {
        Page page=new Page();
        SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
        return page;
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new PinPageException(e,"");
      }
    }
  
  private void add_file_entry(String fileName, PageId pageno) 
    throws AddFileEntryException
    {
      try {
        SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new AddFileEntryException(e,"");
      }      
    }
  
  private void unpinPage(PageId pageno) 
    throws UnpinPageException
    { 
      try{
        SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new UnpinPageException(e,"");
      } 
    }
  
  private void freePage(PageId pageno) 
    throws FreePageException
    {
      try{
	SystemDefs.JavabaseBM.freePage(pageno);    
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new FreePageException(e,"");
      } 
      
    }
  private void delete_file_entry(String filename)
    throws DeleteFileEntryException
    {
      try {
        SystemDefs.JavabaseDB.delete_file_entry( filename );
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new DeleteFileEntryException(e,"");
      } 
    }
  
  private void unpinPage(PageId pageno, boolean dirty) 
    throws UnpinPageException
    {
      try{
        SystemDefs.JavabaseBM.unpinPage(pageno, dirty);  
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new UnpinPageException(e,"");
      }  
    }
  
  
  
  
  /**  LabelBTreeFile class
   * an index file with given filename should already exist; this opens it.
   *@param filename the B+ tree file name. Input parameter.
   *@exception GetFileEntryException  can not ger the file from DB 
   *@exception PinPageException  failed when pin a page
   *@exception ConstructPageException   BT page constructor failed
   */
  public LabelBTreeFile(String filename)
    throws GetFileEntryException,  
	   PinPageException, 
	   ConstructPageException        
    {      
      
      
      headerPageId=get_file_entry(filename);   
      
      headerPage= new  LabelBTreeHeaderPage( headerPageId);       
      dbname = new String(filename);
      /*
       *
       * - headerPageId is the PageId of this LabelBTreeFile's header page;
       * - headerPage, headerPageId valid and pinned
       * - dbname contains a copy of the name of the database
       */
    }    
  
  
  /**
   *  if index file exists, open it; else create it.
   *@param filename file name. Input parameter.
   *@param keytype the type of key. Input parameter.
   *@param keysize the maximum size of a key. Input parameter.
   *@param delete_fashion full delete or naive delete. Input parameter.
   *           It is either DeleteFashion.NAIVE_DELETE or 
   *           DeleteFashion.FULL_DELETE.
   *@exception GetFileEntryException  can not get file
   *@exception ConstructPageException page constructor failed
   *@exception IOException error from lower layer
   *@exception AddFileEntryException can not add file into DB
   */
  public LabelBTreeFile(String filename, int keytype,
		   int keysize, int delete_fashion)  
    throws GetFileEntryException, 
	   ConstructPageException,
	   IOException, 
	   AddFileEntryException
    {
      
      
      headerPageId=get_file_entry(filename);
      if( headerPageId==null) //file not exist
	{
	  headerPage= new LabelBTreeHeaderPage(); 
	  headerPageId= headerPage.getPageId();
	  add_file_entry(filename, headerPageId);
	  headerPage.set_magic0(MAGIC0);
	  headerPage.set_rootId(new PageId(INVALID_PAGE));
	  headerPage.set_keyType((short)keytype);    
	  headerPage.set_maxKeySize(keysize);
	  headerPage.set_deleteFashion( delete_fashion );
	  headerPage.setType(NodeType.BTHEAD);
	}
      else {
	headerPage = new LabelBTreeHeaderPage( headerPageId );  
      }
      
      dbname=new String(filename);
      
    }
  
  /** Close the B+ tree file.  Unpin header page.
   *@exception PageUnpinnedException  error from the lower layer
   *@exception InvalidFrameNumberException  error from the lower layer
   *@exception HashEntryNotFoundException  error from the lower layer
   *@exception ReplacerException  error from the lower layer
   */
  public void close()
    throws PageUnpinnedException, 
	   InvalidFrameNumberException, 
	   HashEntryNotFoundException,
           ReplacerException
    {
      if ( headerPage!=null) {
	SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
	headerPage=null;
      }  
    }
  
  /** Destroy entire B+ tree file.
   *@exception IOException  error from the lower layer
   *@exception IteratorException iterator error
   *@exception UnpinPageException error  when unpin a page
   *@exception FreePageException error when free a page
   *@exception DeleteFileEntryException failed when delete a file from DM
   *@exception ConstructPageException error in BT page constructor 
   *@exception PinPageException failed when pin a page
   */
  public void destroyFile() 
    throws IOException, 
	   IteratorException, 
	   UnpinPageException,
	   FreePageException,   
	   DeleteFileEntryException, 
	   ConstructPageException,
	   PinPageException     
    {
      if( headerPage != null) {
	PageId pgId= headerPage.get_rootId();
	if( pgId.pid != INVALID_PAGE) 
	  _destroyFile(pgId);
	unpinPage(headerPageId);
	freePage(headerPageId);      
	delete_file_entry(dbname);
	headerPage=null;
      }
    }  
  
  
  private void  _destroyFile(PageId pageno) 
    throws IOException, 
	   IteratorException, 
	   PinPageException,
           ConstructPageException, 
	   UnpinPageException, 
	   FreePageException
    {
      
      LabelBTSortedPage sortedPage;
      Page page=pinPage(pageno) ;
      sortedPage= new LabelBTSortedPage( page, headerPage.get_keyType());
      
      if (sortedPage.getType() == NodeType.INDEX) {
	LabelBTIndexPage indexPage= new LabelBTIndexPage( page, headerPage.get_keyType());
	LID      lid=new LID();
	PageId       childId;
	KeyDataEntry entry;
	for (entry = indexPage.getFirst(lid);
	     entry!=null; entry = indexPage.getNext(lid))
	  { 
	    childId = ((IndexData)(entry.data)).getData();
	    _destroyFile(childId);
	  }
      } else { // LabelBTLeafPage 
	
	unpinPage(pageno);
	freePage(pageno);
      }
      
    }
  
  private void  updateHeader(PageId newRoot)
    throws   IOException, 
	     PinPageException,
	     UnpinPageException
    {
      
      LabelBTreeHeaderPage header;
      PageId old_data;
      
      
      header= new LabelBTreeHeaderPage( pinPage(headerPageId));
      
      old_data = headerPage.get_rootId();
      header.set_rootId( newRoot);
      
      // clock in dirty bit to bm so our dtor needn't have to worry about it
      unpinPage(headerPageId, true /* = DIRTY */ );
      
      
      // ASSERTIONS:
      // - headerPage, headerPageId valid, pinned and marked as dirty
      
    }
  
  
  /** insert record with the given key and lid
   *@param key the key of the record. Input parameter.
   *@param lid the rid of the record. Input parameter.
   *@exception  KeyTooLongException key size exceeds the max keysize.
   *@exception KeyNotMatchException key is not integer key nor string key
   *@exception IOException error from the lower layer
   *@exception LeafInsertRecException insert error in leaf page
   *@exception IndexInsertRecException insert error in index page
   *@exception ConstructPageException error in BT page constructor
   *@exception UnpinPageException error when unpin a page
   *@exception PinPageException error when pin a page
   *@exception NodeNotMatchException  node not match index page nor leaf page
   *@exception ConvertException error when convert between revord and byte 
   *             array
   *@exception DeleteRecException error when delete in index page
   *@exception IndexSearchException error when search 
   *@exception IteratorException iterator error
   *@exception LeafDeleteException error when delete in leaf page
   *@exception InsertException  error when insert in index page
   */    
  public void insert(KeyClass key, LID lid) 
    throws KeyTooLongException, 
	   KeyNotMatchException, 
	   LeafInsertRecException,   
	   IndexInsertRecException,
	   ConstructPageException, 
	   UnpinPageException,
	   PinPageException, 
	   NodeNotMatchException, 
	   ConvertException,
	   DeleteRecException,
	   IndexSearchException,
	   IteratorException, 
	   LeafDeleteException, 
	   InsertException,
	   IOException
	   
    {
      KeyDataEntry  newRootEntry;
      
      if (LabelBT.getKeyLength(key) > headerPage.get_maxKeySize())
	throw new KeyTooLongException(null,"");
      
      if ( key instanceof StringKey ) {
	if ( headerPage.get_keyType() != AttrType.attrString ) {
	  throw new KeyNotMatchException(null,"");
	}
      }
      else if ( key instanceof IntegerKey ) {
	if ( headerPage.get_keyType() != AttrType.attrInteger ) {
	  throw new KeyNotMatchException(null,"");
	}
      }   
      else 
	throw new KeyNotMatchException(null,"");
      
      
      // TWO CASES:
      // 1. headerPage.root == INVALID_PAGE:
      //    - the tree is empty and we have to create a new first page;
      //    this page will be a leaf page
      // 2. headerPage.root != INVALID_PAGE:
      //    - we call _insert() to insert the pair (key, lid)
      
      
      if ( trace != null )
	{
	  trace.writeBytes( "INSERT " + lid.pageNo + " "
			    + lid.slotNo + " " + key + lineSep);
	  trace.writeBytes( "DO" + lineSep);
	  trace.flush();
	}
      
      
      if (headerPage.get_rootId().pid == INVALID_PAGE) {
	PageId newRootPageId;
	LabelBTLeafPage newRootPage;
	LID dummylid;
	
	newRootPage=new LabelBTLeafPage( headerPage.get_keyType());
	newRootPageId=newRootPage.getCurPage();
	
	
	if ( trace != null )
	  {
	    trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
	    trace.flush();
	  }
	
	
	
	newRootPage.setNextPage(new PageId(INVALID_PAGE));
	newRootPage.setPrevPage(new PageId(INVALID_PAGE));
	
	
	// ASSERTIONS:
	// - newRootPage, newRootPageId valid and pinned
	
	newRootPage.insertRecord(key, lid); 
	
	if ( trace!=null )
	  {
            trace.writeBytes("PUTIN node " + newRootPageId+lineSep);
            trace.flush();
	  }
	
	unpinPage(newRootPageId, true); /* = DIRTY */
	updateHeader(newRootPageId);
	
	if ( trace!=null )
	  {
            trace.writeBytes("DONE" + lineSep);
            trace.flush();
	  }
        
	
        return;
      }
      
      // ASSERTIONS:
      // - headerPageId, headerPage valid and pinned
      // - headerPage.root holds the pageId of the root of the B-tree
      // - none of the pages of the tree is pinned yet
      
      
      if ( trace != null )
	{
	  trace.writeBytes( "SEARCH" + lineSep);
	  trace.flush();
	}
      
      
      newRootEntry= _insert(key, lid, headerPage.get_rootId());
      
      // TWO CASES:
      // - newRootEntry != null: a leaf split propagated up to the root
      //                            and the root split: the new pageNo is in
      //                            newChildEntry.data.pageNo 
      // - newRootEntry == null: no new root was created;
      //                            information on headerpage is still valid
      
      // ASSERTIONS:
      // - no page pinned
      
      if (newRootEntry != null)
	{
	  LabelBTIndexPage newRootPage;
	  PageId      newRootPageId;
	  Object      newEntryKey;
	  
	  // the information about the pair <key, PageId> is
	  // packed in newRootEntry: extract it
	  
	  newRootPage = new LabelBTIndexPage(headerPage.get_keyType());
	  newRootPageId=newRootPage.getCurPage();
	  
	  // ASSERTIONS:
	  // - newRootPage, newRootPageId valid and pinned
	  // - newEntryKey, newEntryPage contain the data for the new entry
	  //     which was given up from the level down in the recursion
	  
	  
	  if ( trace != null )
	    {
	      trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
	      trace.flush();
	    }
	  
	  
	  newRootPage.insertKey( newRootEntry.key, 
				 ((IndexData)newRootEntry.data).getData() );
	  
	  
	  // the old root split and is now the left child of the new root
	  newRootPage.setPrevPage(headerPage.get_rootId());
	  
	  unpinPage(newRootPageId, true /* = DIRTY */);
	  
	  updateHeader(newRootPageId);
	  
	}
      
      
      if ( trace !=null )
	{
	  trace.writeBytes("DONE"+lineSep);
	  trace.flush();
	}
      
      
      return;
    }
  
  
  
  
  private KeyDataEntry  _insert(KeyClass key, LID lid,  
				PageId currentPageId) 
    throws  PinPageException,  
	    IOException,
	    ConstructPageException, 
	    LeafDeleteException,  
	    ConstructPageException,
	    DeleteRecException, 
	    IndexSearchException,
	    UnpinPageException, 
	    LeafInsertRecException,
	    ConvertException, 
	    IteratorException, 
	    IndexInsertRecException,
	    KeyNotMatchException, 
	    NodeNotMatchException,
	    InsertException 
	    
    {
      
      
      LabelBTSortedPage currentPage;
      Page page;
      KeyDataEntry upEntry;
      
      
      page=pinPage(currentPageId);
      currentPage=new LabelBTSortedPage(page, headerPage.get_keyType());      
      
      if ( trace!=null )
	{
	  trace.writeBytes("VISIT node " + currentPageId+lineSep);
	  trace.flush();
	}
      
      
      // TWO CASES:
      // - pageType == INDEX:
      //   recurse and then split if necessary
      // - pageType == LEAF:
      //   try to insert pair (key, rid), maybe split
      
      if(currentPage.getType() == NodeType.INDEX) {
	LabelBTIndexPage  currentIndexPage=new LabelBTIndexPage(page, 
						      headerPage.get_keyType());
	PageId       currentIndexPageId = currentPageId;
	PageId nextPageId;
	
	nextPageId=currentIndexPage.getPageNoByKey(key);
	
	// now unpin the page, recurse and then pin it again
	unpinPage(currentIndexPageId);
	
	upEntry= _insert(key, lid, nextPageId);
	
	// two cases:
	// - upEntry == null: one level lower no split has occurred:
	//                     we are done.
	// - upEntry != null: one of the children has split and
	//                    upEntry is the new data entry which has
	//                    to be inserted on this index page
	
	if ( upEntry == null)
	  return null;
	
	currentIndexPage= new  LabelBTIndexPage(pinPage(currentPageId),
					   headerPage.get_keyType() );
	
	// ASSERTIONS:
	// - upEntry != null
	// - currentIndexPage, currentIndexPageId valid and pinned
	
	// the information about the pair <key, PageId> is
	// packed in upEntry
	
	// check whether there can still be entries inserted on that page
	if (currentIndexPage.available_space() >=
	    LabelBT.getKeyDataLength( upEntry.key, NodeType.INDEX))
	  {
	    
	    // no split has occurred
	    currentIndexPage.insertKey( upEntry.key, 
					((IndexData)upEntry.data).getData() );
	    
	    unpinPage(currentIndexPageId, true /* DIRTY */);
	    
	    return null;
	  }
	
	// ASSERTIONS:
	// - on the current index page is not enough space available . 
	//   it splits
	
	//   therefore we have to allocate a new index page and we will
	//   distribute the entries
	// - currentIndexPage, currentIndexPageId valid and pinned
	
	LabelBTIndexPage newIndexPage;
	PageId       newIndexPageId;
	
	// we have to allocate a new INDEX page and
	// to redistribute the index entries
	newIndexPage= new LabelBTIndexPage(headerPage.get_keyType());
	newIndexPageId=newIndexPage.getCurPage();  
	
        
	if ( trace !=null )
	  {
	    if (headerPage.get_rootId().pid != currentIndexPageId.pid) 
	      trace.writeBytes("SPLIT node " + currentIndexPageId 
			       + " IN nodes " +  currentIndexPageId +
			       " " + newIndexPageId +lineSep);
	    else
	      trace.writeBytes("ROOTSPLIT IN nodes " + currentIndexPageId 
			       + " " +  newIndexPageId +lineSep);
	    trace.flush();
	  }
	
	
	// ASSERTIONS:
	// - newIndexPage, newIndexPageId valid and pinned
	// - currentIndexPage, currentIndexPageId valid and pinned
	// - upEntry containing (Key, Page) for the new entry which was
	//     given up from the level down in the recursion
	
	KeyDataEntry      tmpEntry;
	PageId       tmpPageId;
	LID insertLid;
	LID delLid=new LID();
	
	for ( tmpEntry= currentIndexPage.getFirst( delLid);
	      tmpEntry!=null;tmpEntry= currentIndexPage.getFirst( delLid))  
	  {
	    newIndexPage.insertKey( tmpEntry.key, 
				    ((IndexData)tmpEntry.data).getData());
	    currentIndexPage.deleteSortedRecord(delLid);
	  }
	
	// ASSERTIONS:
	// - currentIndexPage empty
	// - newIndexPage holds all former records from currentIndexPage
	
	// we will try to make an equal split
	LID firstLid=new LID();
	KeyDataEntry undoEntry=null;
	for (tmpEntry = newIndexPage.getFirst(firstLid);
	     (currentIndexPage.available_space() >
	      newIndexPage.available_space());
	     tmpEntry=newIndexPage.getFirst(firstLid))
	  {
	    // now insert the <key,pageId> pair on the new
	    // index page
	    undoEntry=tmpEntry;
	    currentIndexPage.insertKey( tmpEntry.key, 
					((IndexData)tmpEntry.data).getData());
	    newIndexPage.deleteSortedRecord(firstLid);
          }
	
	//undo the final record
	if ( currentIndexPage.available_space() < 
	     newIndexPage.available_space()) {
	  
	  newIndexPage.insertKey( undoEntry.key, 
				  ((IndexData)undoEntry.data).getData());
	  
	  currentIndexPage.deleteSortedRecord 
	    (new LID(currentIndexPage.getCurPage(),
		     (int)currentIndexPage.getSlotCnt()-1) );              
	}
	
	
	
	// check whether <newKey, newIndexPageId>
	// will be inserted
	// on the newly allocated or on the old index page
	
	tmpEntry= newIndexPage.getFirst(firstLid);
	
	if (LabelBT.keyCompare( upEntry.key, tmpEntry.key) >=0 )
	  {
	    // the new data entry belongs on the new index page
	    newIndexPage.insertKey( upEntry.key, 
				    ((IndexData)upEntry.data).getData());
          }
	else {
	  currentIndexPage.insertKey( upEntry.key, 
				      ((IndexData)upEntry.data).getData());
	  
	  int i= (int)currentIndexPage.getSlotCnt()-1;
	  tmpEntry =
	    LabelBT.getEntryFromBytes(currentIndexPage.getpage(), 
				 currentIndexPage.getSlotOffset(i),
				 currentIndexPage.getSlotLength(i),
				 headerPage.get_keyType(),NodeType.INDEX);

	  newIndexPage.insertKey( tmpEntry.key, 
				  ((IndexData)tmpEntry.data).getData());

	  currentIndexPage.deleteSortedRecord
	    (new LID(currentIndexPage.getCurPage(), i) );      
	  
	}
	
	
	  unpinPage(currentIndexPageId, true /* dirty */);
        
	  // fill upEntry
	  upEntry= newIndexPage.getFirst(delLid);

	  // now set prevPageId of the newIndexPage to the pageId
	  // of the deleted entry:
	  newIndexPage.setPrevPage( ((IndexData)upEntry.data).getData());

	  // delete first record on new index page since it is given up
	  newIndexPage.deleteSortedRecord(delLid);
	  
	  unpinPage(newIndexPageId, true /* dirty */);
	  
	  
	  if ( trace!=null ){
	    trace_children(currentIndexPageId);
	    trace_children(newIndexPageId);
	  }
	  
	  
          ((IndexData)upEntry.data).setData( newIndexPageId);
	  
          return upEntry;  
	  
	  // ASSERTIONS:
	  // - no pages pinned
	  // - upEntry holds the pointer to the KeyDataEntry which is
	  //   to be inserted on the index page one level up
	  
      }
      
      else if ( currentPage.getType()==NodeType.LEAF)
	{
	  LabelBTLeafPage currentLeafPage = 
	    new LabelBTLeafPage(page, headerPage.get_keyType() );

	  PageId currentLeafPageId = currentPageId;
	  
	  // ASSERTIONS:
	  // - currentLeafPage, currentLeafPageId valid and pinned
	  
	  // check whether there can still be entries inserted on that page
	  if (currentLeafPage.available_space() >=
	      LabelBT.getKeyDataLength(key, NodeType.LEAF) )
	    {
	      // no split has occurred
	      
	      currentLeafPage.insertRecord(key, lid); 
	      
	      unpinPage(currentLeafPageId, true /* DIRTY */);
	      
	      
	      if ( trace !=null )
		{
		  trace.writeBytes("PUTIN node "+ currentLeafPageId+lineSep);
		  trace.flush();
		}
	      
	      
	      return null;
	    }
	  
	  // ASSERTIONS:
	  // - on the current leaf page is not enough space available. 
	  //   It splits.
	  // - therefore we have to allocate a new leaf page and we will
	  // - distribute the entries
	  
	  LabelBTLeafPage  newLeafPage;
	  PageId       newLeafPageId;
	  // we have to allocate a new LEAF page and
	  // to redistribute the data entries entries
	  newLeafPage=new LabelBTLeafPage(headerPage.get_keyType());
	  newLeafPageId=newLeafPage.getCurPage();
	  
	  newLeafPage.setNextPage(currentLeafPage.getNextPage());
	  newLeafPage.setPrevPage(currentLeafPageId);  // for dbl-linked list
	  currentLeafPage.setNextPage(newLeafPageId);
	  
	  // change the prevPage pointer on the next page:
	  
	  PageId rightPageId;
	  rightPageId = newLeafPage.getNextPage();
	  if (rightPageId.pid != INVALID_PAGE)
	    {
	      LabelBTLeafPage rightPage;
	      rightPage=new LabelBTLeafPage(rightPageId, headerPage.get_keyType());
	      
	      rightPage.setPrevPage(newLeafPageId);
	      unpinPage(rightPageId, true /* = DIRTY */);
	      
	      // ASSERTIONS:
	      // - newLeafPage, newLeafPageId valid and pinned
	      // - currentLeafPage, currentLeafPageId valid and pinned
	    }
	  
	  if ( trace!=null )
	    {
	      if (headerPage.get_rootId().pid != currentLeafPageId.pid) 
		trace.writeBytes("SPLIT node " + currentLeafPageId 
				 + " IN nodes "
				 + currentLeafPageId + " " + newLeafPageId +lineSep);
	      else
		trace.writeBytes("ROOTSPLIT IN nodes " + currentLeafPageId
				 + " " +  newLeafPageId +lineSep);
	      trace.flush();
	    }
	  
	  
	  
	  KeyDataEntry     tmpEntry;
	  LID       firstLid=new LID();
	  
	  
	  for (tmpEntry = currentLeafPage.getFirst(firstLid);
	       tmpEntry != null;
	       tmpEntry = currentLeafPage.getFirst(firstLid))
	    {
	      
	      newLeafPage.insertRecord( tmpEntry.key,
					((LabelLeafData)(tmpEntry.data)).getData());
	      currentLeafPage.deleteSortedRecord(firstLid);
	      
	    }
	  
	  
	  // ASSERTIONS:
	  // - currentLeafPage empty
	  // - newLeafPage holds all former records from currentLeafPage
	  
	  KeyDataEntry undoEntry=null; 
	  for (tmpEntry = newLeafPage.getFirst(firstLid);
	       newLeafPage.available_space() < 
		 currentLeafPage.available_space(); 
               tmpEntry=newLeafPage.getFirst(firstLid)   )
	    {	   
	      undoEntry=tmpEntry;
	      currentLeafPage.insertRecord( tmpEntry.key,
					    ((LabelLeafData)tmpEntry.data).getData());
	      newLeafPage.deleteSortedRecord(firstLid);		
	    }
	  
	  if (LabelBT.keyCompare(key, undoEntry.key ) <  0) {
	    //undo the final record
	    if ( currentLeafPage.available_space() < 
		 newLeafPage.available_space()) {
	      newLeafPage.insertRecord( undoEntry.key, 
					((LabelLeafData)undoEntry.data).getData());
	      
	      currentLeafPage.deleteSortedRecord
		(new LID(currentLeafPage.getCurPage(),
			 (int)currentLeafPage.getSlotCnt()-1) );              
	    }
	  }	  
	  
	  // check whether <key, rid>
	  // will be inserted
	  // on the newly allocated or on the old leaf page
	  
	  if (LabelBT.keyCompare(key,undoEntry.key ) >= 0)
	    {                     
	      // the new data entry belongs on the new Leaf page
	      newLeafPage.insertRecord(key, lid);
	      
	      
	      if ( trace!=null )
		{
		  trace.writeBytes("PUTIN node " + newLeafPageId +lineSep);
		  trace.flush();
		}
	      
	      
	    }
	  else {
	    currentLeafPage.insertRecord(key,lid);
	  }
	  
	  unpinPage(currentLeafPageId, true /* dirty */);
	  
	  if ( trace!=null ){
	    trace_children(currentLeafPageId);
	    trace_children(newLeafPageId);
	  }
	  
	  
	  
	  // fill upEntry
	  tmpEntry=newLeafPage.getFirst(firstLid);
	  upEntry=new KeyDataEntry(tmpEntry.key, newLeafPageId );
	  
	  
	  unpinPage(newLeafPageId, true /* dirty */);
	  
	  // ASSERTIONS:
	  // - no pages pinned
	  // - upEntry holds the valid KeyDataEntry which is to be inserted 
	  // on the index page one level up
	  return upEntry;
	}
      else {    
	throw new InsertException(null,"");
      }
    }
  
  
  
  
  /** delete leaf entry  given its <key, lid> pair.
   *  `rid' is IN the data entry; it is not the id of the data entry)
   *@param key the key in pair <key, lid>. Input Parameter.
   *@param lid the rid in pair <key, lid>. Input Parameter.
   *@return true if deleted. false if no such record.
   *@exception DeleteFashionException neither full delete nor naive delete
   *@exception LeafRedistributeException redistribution error in leaf pages
   *@exception RedistributeException redistribution error in index pages
   *@exception InsertRecException error when insert in index page
   *@exception KeyNotMatchException key is neither integer key nor string key
   *@exception UnpinPageException error when unpin a page
   *@exception IndexInsertRecException  error when insert in index page
   *@exception FreePageException error in BT page constructor
   *@exception RecordNotFoundException error delete a record in a BT page
   *@exception PinPageException error when pin a page
   *@exception IndexFullDeleteException  fill delete error
   *@exception LeafDeleteException delete error in leaf page
   *@exception IteratorException iterator error
   *@exception ConstructPageException error in BT page constructor
   *@exception DeleteRecException error when delete in index page
   *@exception IndexSearchException error in search in index pages
   *@exception IOException error from the lower layer
   *
   */
  public boolean Delete(KeyClass key, LID lid) 
    throws  DeleteFashionException, 
	    LeafRedistributeException,
	    RedistributeException,
	    InsertRecException,
	    KeyNotMatchException, 
	    UnpinPageException, 
	    IndexInsertRecException,
	    FreePageException, 
	    RecordNotFoundException, 
	    PinPageException,
	    IndexFullDeleteException, 
	    LeafDeleteException,
	    IteratorException, 
	    ConstructPageException, 
	    DeleteRecException,
	    IndexSearchException, 
	    IOException
    {
      if (headerPage.get_deleteFashion() ==DeleteFashion.FULL_DELETE) 
        return FullDelete(key, lid); 
      else if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
        return NaiveDelete(key, lid);
      else
	throw new DeleteFashionException(null,"");
    }
  
  
  
  
  /* 
   * findRunStart.
   * Status LabelBTreeFile::findRunStart (const void   lo_key,
   *                                LID          *pstartlid)
   *
   * find left-most occurrence of `lo_key', going all the way left if
   * lo_key is null.
   * 
   * Starting record returned in *pstartrid, on page *pppage, which is pinned.
   *
   * Since we allow duplicates, this must "go left" as described in the text
   * (for the search algorithm).
   *@param lo_key  find left-most occurrence of `lo_key', going all 
   *               the way left if lo_key is null.
   *@param startrid it will reurn the first rid =< lo_key
   *@return return a BTLeafPage instance which is pinned. 
   *        null if no key was found.
   */
  
  LabelBTLeafPage findRunStart (KeyClass lo_key, 
			   LID startlid)
    throws IOException, 
	   IteratorException,  
	   KeyNotMatchException,
	   ConstructPageException, 
	   PinPageException, 
	   UnpinPageException
    {
      LabelBTLeafPage  pageLeaf;
      LabelBTIndexPage pageIndex;
      Page page;
      LabelBTSortedPage  sortPage;
      PageId pageno;
      PageId curpageno=null;                // iterator
      PageId prevpageno;
      PageId nextpageno;
      LID curLid;
      KeyDataEntry curEntry;
      
      pageno = headerPage.get_rootId();
      
      if (pageno.pid == INVALID_PAGE){        // no pages in the BTREE
        pageLeaf = null;                // should be handled by 
        // startrid =INVALID_PAGEID ;             // the caller
        return pageLeaf;
      }
      
      page= pinPage(pageno);
      sortPage=new LabelBTSortedPage(page, headerPage.get_keyType());
      
      
      if ( trace!=null ) {
	trace.writeBytes("VISIT node " + pageno + lineSep);
	trace.flush();
      }
      
      
      // ASSERTION
      // - pageno and sortPage is the root of the btree
      // - pageno and sortPage valid and pinned
      
      while (sortPage.getType() == NodeType.INDEX) {
	pageIndex=new LabelBTIndexPage(page, headerPage.get_keyType()); 
	prevpageno = pageIndex.getPrevPage();
	curEntry= pageIndex.getFirst(startlid);
	while ( curEntry!=null && lo_key != null 
		&& LabelBT.keyCompare(curEntry.key, lo_key) < 0) {
	  
          prevpageno = ((IndexData)curEntry.data).getData();
          curEntry=pageIndex.getNext(startlid);
	}
	
	unpinPage(pageno);
	
	pageno = prevpageno;
	page=pinPage(pageno);
	sortPage=new LabelBTSortedPage(page, headerPage.get_keyType()); 
	
	
	if ( trace!=null )
	  {
	    trace.writeBytes( "VISIT node " + pageno+lineSep);
	    trace.flush();
	  }
	
	
      }
      
      pageLeaf = new LabelBTLeafPage(page, headerPage.get_keyType() );
      
      curEntry=pageLeaf.getFirst(startlid);
      while (curEntry==null) {
	// skip empty leaf pages off to left
	nextpageno = pageLeaf.getNextPage();
	unpinPage(pageno);
	if (nextpageno.pid == INVALID_PAGE) {
	  // oops, no more records, so set this scan to indicate this.
	  return null;
	}
	
	pageno = nextpageno; 
	pageLeaf=  new LabelBTLeafPage( pinPage(pageno), headerPage.get_keyType());    
	curEntry=pageLeaf.getFirst(startlid);
      }
      
      // ASSERTIONS:
      // - curkey, curLid: contain the first record on the
      //     current leaf page (curkey its key, cur
      // - pageLeaf, pageno valid and pinned
      
      
      if (lo_key == null) {
	return pageLeaf;
	// note that pageno/pageLeaf is still pinned; 
	// scan will unpin it when done
      }
      
      while (LabelBT.keyCompare(curEntry.key, lo_key) < 0) {
	curEntry= pageLeaf.getNext(startlid);
	while (curEntry == null) { // have to go right
	  nextpageno = pageLeaf.getNextPage();
	  unpinPage(pageno);
	  
	  if (nextpageno.pid == INVALID_PAGE) {
	    return null;
	  }
	  
	  pageno = nextpageno;
	  pageLeaf=new LabelBTLeafPage(pinPage(pageno), headerPage.get_keyType());
	  
	  curEntry=pageLeaf.getFirst(startlid);
	}
      }
      
      return pageLeaf;
    }
  
  
  
  /*
   *  Status LabelBTreeFile::NaiveDelete (const void *key, const LID lid) 
   * 
   * Remove specified data entry (<key, lid>) from an index.
   *
   * We don't do merging or redistribution, but do allow duplicates.
   *
   * Page containing first occurrence of key `key' is found for us
   * by findRunStart.  We then iterate for (just a few) pages, if necesary,
   * to find the one containing <key,lid>, which we then delete via
   * BTLeafPage::delUserLid.
   */
  
  private boolean NaiveDelete ( KeyClass key, LID lid)
    throws LeafDeleteException,  
	   KeyNotMatchException,  
	   PinPageException,
	   ConstructPageException, 
	   IOException,
	   UnpinPageException,  
	   PinPageException, 
	   IndexSearchException,  
	   IteratorException
    {
      LabelBTLeafPage leafPage;
      LID curLid=new LID();  // iterator
      KeyClass curkey;
      LID dummyLid; 
      PageId nextpage;
      boolean deleted;
      KeyDataEntry entry;
      
      if ( trace!=null )
	{
          trace.writeBytes("DELETE " +lid.pageNo +" " + lid.slotNo + " " 
			   + key +lineSep);
          trace.writeBytes( "DO"+lineSep);
          trace.writeBytes( "SEARCH" +lineSep);
          trace.flush();
	}
      
      
      leafPage=findRunStart(key, curLid);  // find first page,rid of key
      if( leafPage == null) return false;
      
      entry=leafPage.getCurrent(curLid);
      
      while ( true ) {
	
        while ( entry == null) { // have to go right
	  nextpage = leafPage.getNextPage();
	  unpinPage(leafPage.getCurPage());
	  if (nextpage.pid == INVALID_PAGE) {
	    return false;
	  }
	  
	  leafPage=new LabelBTLeafPage(pinPage(nextpage), 
				  headerPage.get_keyType() );
	  entry=leafPage.getFirst(new LID());
	}
	
	if ( LabelBT.keyCompare(key, entry.key) > 0 )
	  break;
	
	if( leafPage.delEntry(new KeyDataEntry(key, lid)) ==true) {
	  
          // successfully found <key, rid> on this page and deleted it.
          // unpin dirty page and return OK.
          unpinPage(leafPage.getCurPage(), true /* = DIRTY */);
	  
	  
          if ( trace!=null )
	    {
	      trace.writeBytes( "TAKEFROM node " + leafPage.getCurPage()+lineSep);
	      trace.writeBytes("DONE"+lineSep);
	      trace.flush();
	    }
          
	  
          return true;
	}
	
	nextpage = leafPage.getNextPage();
	unpinPage(leafPage.getCurPage());
	
	leafPage=new LabelBTLeafPage(pinPage(nextpage), headerPage.get_keyType());
	
	entry=leafPage.getFirst(curLid);
      }
      
      /*
       * We reached a page with first key > `key', so return an error.
       * We should have got true back from delUserRid above.  Apparently
       * the specified <key,lid> data entry does not exist.
       */
      
      unpinPage(leafPage.getCurPage());
      return false;
    }

  
  /*
   * Status LabelBTreeFile::FullDelete (const void *key, const LID lid) 
   * 
   * Remove specified data entry (<key, lid>) from an index.
   *
   * Most work done recursively by _Delete
   *
   * Special case: delete root if the tree is empty
   *
   * Page containing first occurrence of key `key' is found for us
   * After the page containing first occurence of key 'key' is found,
   * we iterate for (just a few) pages, if necesary,
   * to find the one containing <key,rid>, which we then delete via
   * BTLeafPage::delUserRid.
   *@return false if no such record; true if succees 
   */
  
  private boolean FullDelete (KeyClass key,  LID lid)
    throws IndexInsertRecException,
	   RedistributeException,
	   IndexSearchException, 
	   RecordNotFoundException, 
	   DeleteRecException,
	   InsertRecException, 
	   LeafRedistributeException, 
	   IndexFullDeleteException,
	   FreePageException, 
	   LeafDeleteException, 
	   KeyNotMatchException, 
	   ConstructPageException, 
	   IOException, 
	   IteratorException,
	   PinPageException, 
	   UnpinPageException, 
	   IteratorException
    {
      
      try {
	
	if ( trace !=null)
	  {
	    trace.writeBytes( "DELETE " + lid.pageNo + " " + lid.slotNo 
			      + " " +  key +lineSep);
	    trace.writeBytes("DO"+lineSep);
	    trace.writeBytes( "SEARCH"+lineSep);
	    trace.flush();
	  }
	
	
	_Delete(key, lid, headerPage.get_rootId(), null);
	
	
	if ( trace !=null)
	  {
	    trace.writeBytes("DONE"+lineSep);
	    trace.flush();
	  }
	
	
	return true;
      }
      catch (RecordNotFoundException e) {
	return false;
      }
      
    }
  
  private KeyClass _Delete ( KeyClass key,
			     LID     lid,
			     PageId        currentPageId,
			     PageId        parentPageId)
    throws IndexInsertRecException,
	   RedistributeException,
	   IndexSearchException, 
	   RecordNotFoundException, 
	   DeleteRecException,
	   InsertRecException, 
	   LeafRedistributeException, 
	   IndexFullDeleteException,
	   FreePageException, 
	   LeafDeleteException, 
	   KeyNotMatchException, 
	   ConstructPageException, 
	   UnpinPageException, 
	   IteratorException,
	   PinPageException,   
	   IOException     
    {
      
      LabelBTSortedPage  sortPage;
      Page page;
      page=pinPage(currentPageId);
      sortPage=new LabelBTSortedPage(page, headerPage.get_keyType());
      
      
      if ( trace!=null )
	{
	  trace.writeBytes("VISIT node " + currentPageId +lineSep);
	  trace.flush();
	}
      
      
      if (sortPage.getType()==NodeType.LEAF ) {
        LID curLid=new LID();  // iterator
        KeyDataEntry tmpEntry;
        KeyClass curkey;
        LID dummyLid; 
        PageId nextpage;
        LabelBTLeafPage leafPage;
        leafPage=new LabelBTLeafPage(page, headerPage.get_keyType());        
	
	
	KeyClass deletedKey=key;
	tmpEntry=leafPage.getFirst(curLid);
	
	LID delLid;    
	// for all records with key equal to 'key', delete it if its lid = 'lid'
	while((tmpEntry!=null) && (LabelBT.keyCompare(key,tmpEntry.key)>=0)) { 
          // WriteUpdateLog is done in the btleafpage level - to log the
          // deletion of the lid.
	  
	  if ( leafPage.delEntry(new KeyDataEntry(key, lid)) ) {
	    // successfully found <key, lid> on this page and deleted it.
	    
	    
	    if ( trace!=null )
	      {
		trace.writeBytes("TAKEFROM node "+leafPage.getCurPage()+lineSep);
		trace.flush();
	      }
	    
	    PageId leafPage_no=leafPage.getCurPage();     
	    if ( (4+leafPage.available_space()) <= 
		 ((MAX_SPACE-HFPage.DPFIXED)/2) ) { 
	      // the leaf page is at least half full after the deletion
	      unpinPage(leafPage.getCurPage(), true /* = DIRTY */);
	      return null;
	    }
	    else if (leafPage_no.pid == headerPage.get_rootId().pid) {
	      // the tree has only one node - the root
	      if (leafPage.numberOfRecords() != 0) {
		unpinPage(leafPage_no, true /*= DIRTY */);
		return null;
	      }
	      else {
		// the whole tree is empty
		
		if ( trace!=null )
		  {
		    trace.writeBytes("DEALLOCATEROOT " 
				     + leafPage_no+lineSep);
		    trace.flush();
		  }
                
		freePage(leafPage_no);
		
		updateHeader(new PageId(INVALID_PAGE) );
		return null;
	      }
            }
            else { 
	      // get a sibling
	      LabelBTIndexPage  parentPage;
	      parentPage =
		new LabelBTIndexPage(pinPage(parentPageId), 
				headerPage.get_keyType());
	      
	      PageId siblingPageId=new PageId();
	      LabelBTLeafPage siblingPage;
	      int direction;
	      direction=parentPage.getSibling(key, siblingPageId);
	      
	      
	      if (direction==0) {
		// there is no sibling. nothing can be done. 
		
		unpinPage(leafPage.getCurPage(), true /*=DIRTY*/);
		
		unpinPage(parentPageId);
		
		return null;
              }
	      
              siblingPage=new LabelBTLeafPage(pinPage(siblingPageId), 
					 headerPage.get_keyType());
	      
	      
	      
              if (siblingPage.redistribute( leafPage, parentPage,
					    direction, deletedKey)) {
		// the redistribution has been done successfully
		
		if ( trace!=null ){
		  
		  trace_children(leafPage.getCurPage());
		  trace_children(siblingPage.getCurPage());
		  
		}
		
		unpinPage(leafPage.getCurPage(), true);
		unpinPage(siblingPageId, true);      
		unpinPage(parentPageId, true);
		return null;
              }
              else if ( (siblingPage.available_space() + 8 /* 2*sizeof(slot) */ ) >=
			( (MAX_SPACE-HFPage.DPFIXED) 
			  - leafPage.available_space())) {
		
		// we can merge these two children
		// get old child entry in the parent first
		KeyDataEntry   oldChildEntry;
		if (direction==-1)
		  oldChildEntry = leafPage.getFirst(curLid);
		// get a copy
		else {
		  oldChildEntry=siblingPage.getFirst(curLid);
		}
		
		// merge the two children
		LabelBTLeafPage leftChild, rightChild;
		if (direction==-1) {
		  leftChild = siblingPage;
		  rightChild = leafPage;
		}
		else {
		  leftChild = leafPage;
		  rightChild = siblingPage;
		}
		
		// move all entries from rightChild to leftChild
		LID firstLid=new LID(), insertLid;
		for (tmpEntry= rightChild.getFirst(firstLid);
		     tmpEntry != null;
		     tmpEntry=rightChild.getFirst(firstLid)) {
		  leftChild.insertRecord(tmpEntry);
		  rightChild.deleteSortedRecord(firstLid);
		}
		
		// adjust chain
		leftChild.setNextPage(rightChild.getNextPage());
		if ( rightChild.getNextPage().pid != INVALID_PAGE) {
		  LabelBTLeafPage nextLeafPage=new LabelBTLeafPage(
							 rightChild.getNextPage(), headerPage.get_keyType());
		  nextLeafPage.setPrevPage(leftChild.getCurPage());
		  unpinPage( nextLeafPage.getCurPage(), true);
		}
		
		
		if ( trace!=null )
		  {
		    trace.writeBytes("MERGE nodes "+
				     leftChild.getCurPage() 
				     + " " + rightChild.getCurPage()+lineSep);
		    trace.flush();
		  }
		
		
		unpinPage(leftChild.getCurPage(), true);
		
		unpinPage(parentPageId, true);
		
		freePage(rightChild.getCurPage());
		
		return  oldChildEntry.key;
	      }
	      else {
                // It's a very rare case when we can do neither
                // redistribution nor merge. 
		
		
                unpinPage(leafPage.getCurPage(), true);
                
                unpinPage(siblingPageId, true);
		
                unpinPage(parentPageId, true);
                
		
                return null;
	      }
	    } //get a sibling block
	  }// delete success block
	  
	  nextpage = leafPage.getNextPage();
	  unpinPage(leafPage.getCurPage());
	  
	  if (nextpage.pid == INVALID_PAGE ) 
            throw  new RecordNotFoundException(null,"");
	  
	  leafPage=new LabelBTLeafPage(pinPage(nextpage), headerPage.get_keyType() );
	  tmpEntry=leafPage.getFirst(curLid);
	  
	} //while loop
	
	/*
	 * We reached a page with first key > `key', so return an error.
	 * We should have got true back from delUserRid above.  Apparently
	 * the specified <key,rid> data entry does not exist.
	 */
	
	unpinPage(leafPage.getCurPage());
	throw  new RecordNotFoundException(null,""); 
      }
      
      
      if (  sortPage.getType() == NodeType.INDEX ) {
	PageId childPageId;
	LabelBTIndexPage indexPage=new LabelBTIndexPage(page, headerPage.get_keyType());
	childPageId= indexPage.getPageNoByKey(key);
	
	// now unpin the page, recurse and then pin it again
	unpinPage(currentPageId);
	
	KeyClass oldChildKey= _Delete(key, lid,  childPageId, currentPageId);
	
	// two cases:
	// - oldChildKey == null: one level lower no merge has occurred:
	// - oldChildKey != null: one of the children has been deleted and
	//                     oldChildEntry is the entry to be deleted.
	
	indexPage=new LabelBTIndexPage(pinPage(currentPageId), headerPage.get_keyType());
	
	if (oldChildKey ==null) {
	  unpinPage(indexPage.getCurPage(),true);
	  return null;
	}
	
	// delete the oldChildKey
	
	// save possible old child entry before deletion
	PageId dummyPageId; 
	KeyClass deletedKey = key;
	LID curLid=indexPage.deleteKey(oldChildKey);
	
	if (indexPage.getCurPage().pid == headerPage.get_rootId().pid) {
	  // the index page is the root
	  if (indexPage.numberOfRecords() == 0) {
	    LabelBTSortedPage childPage;
	    childPage=new LabelBTSortedPage(indexPage.getPrevPage(),
				       headerPage.get_keyType()); 
	    
	    
	    if ( trace !=null )
	      {
		trace.writeBytes( "CHANGEROOT from node " +
				  indexPage.getCurPage()
				  + " to node " +indexPage.getPrevPage()+lineSep);
		trace.flush();
	      }
	    
	    
	    updateHeader(indexPage.getPrevPage());
	    unpinPage(childPage.getCurPage());
	    
	    freePage(indexPage.getCurPage());
	    return null;
	  }
	  unpinPage(indexPage.getCurPage(),true);
	  return null;
	}
	
	// now we know the current index page is not a root
	if ((4 /*sizeof slot*/ +indexPage.available_space()) <= 
	    ((MAX_SPACE-HFPage.DPFIXED)/2)) {
	  // the index page is at least half full after the deletion
	  unpinPage(currentPageId,true);
	  
	  return null;
	}
	else {
	  // get a sibling
	  LabelBTIndexPage  parentPage;
	  parentPage=new LabelBTIndexPage(pinPage(parentPageId), 
				     headerPage.get_keyType()); 
	  
	  PageId siblingPageId=new PageId();
	  LabelBTIndexPage siblingPage;
	  int direction;
	  direction=parentPage.getSibling(key,
					  siblingPageId);
	  if ( direction==0) {
	    // there is no sibling. nothing can be done.
	    
	    unpinPage(indexPage.getCurPage(), true);
	    
	    unpinPage(parentPageId);
	    
	    return null;
	  }
	  
	  siblingPage=new LabelBTIndexPage( pinPage(siblingPageId), 
				       headerPage.get_keyType());
	  
	  int pushKeySize=0;
	  if (direction==1) {
	    pushKeySize=LabelBT.getKeyLength
	      (parentPage.findKey(siblingPage.getFirst(new LID()).key));
	  } else if (direction==-1) {
	    pushKeySize=LabelBT.getKeyLength
	      (parentPage.findKey(indexPage.getFirst(new LID()).key));
	  }  
	  
	  if (siblingPage.redistribute(indexPage,parentPage, 
				       direction, deletedKey)) {
	    // the redistribution has been done successfully
	    
	    
	    if ( trace!=null ){
	      
	      trace_children(indexPage.getCurPage());
	      trace_children(siblingPage.getCurPage());
	      
	    }
	    
	    
	    unpinPage(indexPage.getCurPage(), true);
	    
	    unpinPage(siblingPageId, true);
	    
	    unpinPage(parentPageId, true);
	    
	    
	    return null;
	  }
	  else if ( siblingPage.available_space()+4 /*slot size*/ >=
		    ( (MAX_SPACE-HFPage.DPFIXED) - 
		      (indexPage.available_space()+4 /*slot size*/)
		      +pushKeySize+4 /*slot size*/ + 4 /* pageId size*/)  ) { 
            
	    
	    // we can merge these two children 
	    
	    // get old child entry in the parent first
	    KeyClass oldChildEntry;
	    if (direction==-1) {
	      oldChildEntry=indexPage.getFirst(curLid).key;
	    }
	    else {
	      oldChildEntry= siblingPage.getFirst(curLid).key;                    
	    }
	    
	    // merge the two children
	    LabelBTIndexPage leftChild, rightChild;
	    if (direction==-1) {
	      leftChild = siblingPage;
	      rightChild = indexPage;
	    }
	    else {
	      leftChild = indexPage;
	      rightChild = siblingPage;
	    }
	    
	    if ( trace!= null )
	      {
		trace.writeBytes( "MERGE nodes " + leftChild.getCurPage() 
				  + " "
				  + rightChild.getCurPage()+lineSep);
		trace.flush();
	      }
	    
	    // pull down the entry in its parent node
	    // and put it at the end of the left child
	    LID firstLid=new LID(), insertLid;
	    PageId curPageId;
	    
	    leftChild.insertKey(  parentPage.findKey(oldChildEntry),
                                  rightChild.getLeftLink());
	    
	    // move all entries from rightChild to leftChild
	    for (KeyDataEntry tmpEntry=rightChild.getFirst(firstLid);
		 tmpEntry != null;
		 tmpEntry=rightChild.getFirst(firstLid) ) {
	      leftChild.insertKey(tmpEntry.key, 
                                  ((IndexData)tmpEntry.data).getData());
	      rightChild.deleteSortedRecord(firstLid); 
	    }
	    
	    unpinPage(leftChild.getCurPage(), true);
	    
	    unpinPage(parentPageId, true);
	    
	    freePage(rightChild.getCurPage());
	    
	    return oldChildEntry;  // ??? 
	    
	  }
	  else {
	    // It's a very rare case when we can do neither
	    // redistribution nor merge. 
	    
	    
	    unpinPage(indexPage.getCurPage(), true);
	    
	    unpinPage(siblingPageId, true);
	    
	    unpinPage(parentPageId);
	    
	    return null;
	  }
	}
      } //index node 
      return null; //neither leaf and index page
      
    }
  
  
  
  /** create a scan with given keys
   * Cases:
   *      (1) lo_key = null, hi_key = null
   *              scan the whole index
   *      (2) lo_key = null, hi_key!= null
   *              range scan from min to the hi_key
   *      (3) lo_key!= null, hi_key = null
   *              range scan from the lo_key to max
   *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
   *              exact match ( might not unique)
   *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
   *              range scan from lo_key to hi_key
   *@param lo_key the key where we begin scanning. Input parameter.
   *@param hi_key the key where we stop scanning. Input parameter.
   *@exception IOException error from the lower layer
   *@exception KeyNotMatchException key is not integer key nor string key
   *@exception IteratorException iterator error
   *@exception ConstructPageException error in BT page constructor
   *@exception PinPageException error when pin a page
   *@exception UnpinPageException error when unpin a page
   */
  public LabelBTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
    throws IOException,  
	   KeyNotMatchException, 
	   IteratorException, 
	   ConstructPageException, 
	   PinPageException, 
	   UnpinPageException
	   
    {
      LabelBTFileScan scan = new LabelBTFileScan();
      if ( headerPage.get_rootId().pid==INVALID_PAGE) {
	scan.leafPage=null;
	return scan;
      }
      
      scan.treeFilename=dbname;
      scan.endkey=hi_key;
      scan.didfirst=false;
      scan.deletedcurrent=false;
      scan.curLid=new LID();     
      scan.keyType=headerPage.get_keyType();
      scan.maxKeysize=headerPage.get_maxKeySize();
      scan.bfile=this;
      
      //this sets up scan at the starting position, ready for iteration
      scan.leafPage=findRunStart( lo_key, scan.curLid);
      return scan;
    }
  
  void trace_children(PageId id)
    throws  IOException, 
	    IteratorException, 
	    ConstructPageException,
	    PinPageException, 
	    UnpinPageException
    {
      
      if( trace!=null ) {
	
	LabelBTSortedPage sortedPage;
	LID metaLid=new LID();
	PageId childPageId;
	KeyClass key;
	KeyDataEntry entry;
	sortedPage=new LabelBTSortedPage( pinPage( id), headerPage.get_keyType());
	
	
	// Now print all the child nodes of the page.  
	if( sortedPage.getType()==NodeType.INDEX) {
	  LabelBTIndexPage indexPage=new LabelBTIndexPage(sortedPage,headerPage.get_keyType()); 
	  trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
	  trace.writeBytes( " " + indexPage.getPrevPage());
	  for ( entry = indexPage.getFirst( metaLid );
		entry != null;
		entry = indexPage.getNext( metaLid ) )
	    {
	      trace.writeBytes( "   " + ((IndexData)entry.data).getData());
	    }
	}
	else if( sortedPage.getType()==NodeType.LEAF) {
	  LabelBTLeafPage leafPage=new LabelBTLeafPage(sortedPage,headerPage.get_keyType()); 
	  trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
	  for ( entry = leafPage.getFirst( metaLid );
		entry != null;
		entry = leafPage.getNext( metaLid ) )
	    {
	      trace.writeBytes( "   " + entry.key + " " + entry.data);
	    }
	}
	unpinPage( id );
	trace.writeBytes(lineSep);
	trace.flush();
      }
      
    }
  
}


