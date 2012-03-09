package labelheap;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;

interface  Filetype {
	  int TEMP = 0;
	  int ORDINARY = 1;
	  
	} // end of Filetype

//We maintain a similar structure to the Heapfile already present
public class LabelHeapfile implements Filetype,  GlobalConst {
	  
	  
	  PageId      _firstDirPageId;   // page number of header page
	  int         _ftype;
	  private     boolean     _file_deleted;
	  private     String 	 _fileName;
	  private static int tempfilecount = 0;
	  
	  
	  
	  /* get a new datapage from the buffer manager and initialize dpinfo
	     @param dpinfop the information in the new HFPage
	  */
	  private LHFPage _newDatapage(DataPageInfo dpinfop)
	    throws LHFException,
		   LHFBufMgrException,
		   LHFDiskMgrException,
		   IOException
	    {
	      Page apage = new Page();
	      PageId pageId = new PageId();
	      pageId = newPage(apage, 1);
	      
	      if(pageId == null)
		throw new LHFException(null, "can't create a new page");
	      
	      // initialize internal values of the new page:
	      
	      LHFPage hfpage = new LHFPage();
	      hfpage.init(pageId, apage);
	      
	      dpinfop.pageId.pid = pageId.pid;
	      dpinfop.recct = 0;
	      dpinfop.availspace = hfpage.available_space();
	      
	      return hfpage;
	      
	    } // end of _newDatapage
	  
	  /* Internal HeapFile function (used in getRecord and updateRecord):
	     returns pinned directory page and pinned data page of the specified 
	     user record(rid) and true if record is found.
	     If the user record cannot be found, return false.
	  */
	  private boolean  _findDataPage( LID lid,
					  PageId dirPageId, LHFPage dirpage,
					  PageId dataPageId, LHFPage datapage,
					  LID rpDataPageRid) 
	    throws InvalidSlotNumberException, 
		   InvalidLabelSizeException, 
		   LHFException,
		   LHFBufMgrException,
		   LHFDiskMgrException,
		   Exception
	    {
	      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
	      
	      LHFPage currentDirPage = new LHFPage();
	      LHFPage currentDataPage = new LHFPage();
	      LID currentDataPageRid = new LID();
	      PageId nextDirPageId = new PageId();
	      // datapageId is stored in dpinfo.pageId 
	      
	      
	      pinPage(currentDirPageId, currentDirPage, false/*read disk*/);
	      
	      Label atuple = new Label();
	      
	      while (currentDirPageId.pid != INVALID_PAGE)
		{// Start While01
		  // ASSERTIONS:
		  //  currentDirPage, currentDirPageId valid and pinned and Locked.
		  
		  for( currentDataPageRid = currentDirPage.firstRecord();
		       currentDataPageRid != null;
		       currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid))
		    {
		      try{
			atuple = currentDirPage.getRecord(currentDataPageRid);
		      }
		      catch (InvalidSlotNumberException e)// check error! return false(done) 
			{
			  return false;
			}
		      
		      DataPageInfo dpinfo = new DataPageInfo(atuple);
		      try{
			pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);
			
			
			//check error;need unpin currentDirPage
		      }catch (Exception e)
			{
			  unpinPage(currentDirPageId, false/*undirty*/);
			  dirpage = null;
			  datapage = null;
			  throw e;
			}
		      
		      
		      
		      // ASSERTIONS:
		      // - currentDataPage, currentDataPageRid, dpinfo valid
		      // - currentDataPage pinned
		      
		      if(dpinfo.pageId.pid==lid.pageNo.pid)
			{
			  atuple = currentDataPage.returnRecord(lid);
			  // found user's record on the current datapage which itself
			  // is indexed on the current dirpage.  Return both of these.
			  
			  dirpage.setpage(currentDirPage.getpage());
			  dirPageId.pid = currentDirPageId.pid;
			  
			  datapage.setpage(currentDataPage.getpage());
			  dataPageId.pid = dpinfo.pageId.pid;
			  
			  rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
			  rpDataPageRid.slotNo = currentDataPageRid.slotNo;
			  return true;
			}
		      else
			{
			  // user record not found on this datapage; unpin it
			  // and try the next one
			  unpinPage(dpinfo.pageId, false /*undirty*/);
			  
			}
		      
		    }
		  
		  // if we would have found the correct datapage on the current
		  // directory page we would have already returned.
		  // therefore:
		  // read in next directory page:
		  
		  nextDirPageId = currentDirPage.getNextPage();
		  try{
		    unpinPage(currentDirPageId, false /*undirty*/);
		  }
		  catch(Exception e) {
		    throw new LHFException (e, "heapfile,_find,unpinpage failed");
		  }
		  
		  currentDirPageId.pid = nextDirPageId.pid;
		  if(currentDirPageId.pid != INVALID_PAGE)
		    {
		      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
		      if(currentDirPage == null)
			throw new LHFException(null, "pinPage return null page");  
		    }
		  
		  
		} // end of While01
	      // checked all dir pages and all data pages; user record not found:(
	      
	      dirPageId.pid = dataPageId.pid = INVALID_PAGE;
	      
	      return false;   
	      
	      
	    } // end of _findDatapage		     
	  
	  /** Initialize.  A null name produces a temporary heapfile which will be
	   * deleted by the destructor.  If the name already denotes a file, the
	   * file is opened; otherwise, a new empty file is created.
	   *
	   * @exception HFException heapfile exception
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception IOException I/O errors
	   */
	  public  LabelHeapfile(String name) 
	    throws LHFException, 
		   LHFBufMgrException,
		   LHFDiskMgrException,
		   IOException
		   
	    {
	      // Give us a prayer of destructing cleanly if construction fails.
	      _file_deleted = true;
	      _fileName = null;
	      //System.out.println("("+name+")\n");
	      if(name == null) 
		{
		  // If the name is NULL, allocate a temporary name
		  // and no logging is required.
		  _fileName = "tempHeapFile";
		  String useId = new String("user.name");
		  String userAccName;
		  userAccName = System.getProperty(useId);
		  _fileName = _fileName + userAccName;
		  
		  String filenum = Integer.toString(tempfilecount);
		  _fileName = _fileName + filenum; 
		  _ftype = TEMP;
		  tempfilecount ++;
		  
		}
	      else
		{
		  _fileName = name;
		  _ftype = ORDINARY;    
		}
	      
	      // The constructor gets run in two different cases.
	      // In the first case, the file is new and the header page
	      // must be initialized.  This case is detected via a failure
	      // in the db->get_file_entry() call.  In the second case, the
	      // file already exists and all that must be done is to fetch
	      // the header page into the buffer pool
	      
	      // try to open the file
	      
	      Page apage = new Page();
	      _firstDirPageId = null;
	      if (_ftype == ORDINARY)
		_firstDirPageId = get_file_entry(_fileName);
	      
	      if(_firstDirPageId==null)
		{
		  // file doesn't exist. First create it.
		  _firstDirPageId = newPage(apage, 1);
		  // check error
		  if(_firstDirPageId == null)
		    throw new LHFException(null, "can't new page");
		  
		  add_file_entry(_fileName, _firstDirPageId);
		  // check error(new exception: Could not add file entry
		  
		  LHFPage firstDirPage = new LHFPage();
		  firstDirPage.init(_firstDirPageId, apage);
		  PageId pageId = new PageId(INVALID_PAGE);
		  
		  firstDirPage.setNextPage(pageId);
		  firstDirPage.setPrevPage(pageId);
		  unpinPage(_firstDirPageId, true /*dirty*/ );
		  
		  
		}
	      _file_deleted = false;
	      // ASSERTIONS:
	      // - ALL private data members of class Heapfile are valid:
	      //
	      //  - _firstDirPageId valid
	      //  - _fileName valid
	      //  - no datapage pinned yet    
	      
	    } // end of constructor 
	  
	  /** Return number of records in file.
	   *
	   * @exception InvalidSlotNumberException invalid slot number
	   * @exception InvalidLabelSizeException invalid tuple size
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception IOException I/O errors
	   */
	  public int getRecCnt() 
	    throws InvalidSlotNumberException, 
		   InvalidLabelSizeException, 
		   LHFDiskMgrException,
		   LHFBufMgrException,
		   IOException
		   
	    {
	      int answer = 0;
	      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
	      
	      PageId nextDirPageId = new PageId(0);
	      
	      LHFPage currentDirPage = new LHFPage();
	      Page pageinbuffer = new Page();
	      
	      while(currentDirPageId.pid != INVALID_PAGE)
		{
		   pinPage(currentDirPageId, currentDirPage, false);
		   
		   LID lid = new LID();
		   Label atuple;
		   for (lid = currentDirPage.firstRecord();
		        lid != null;	// lid==NULL means no more record
		        lid = currentDirPage.nextRecord(lid))
		     {
		       atuple = currentDirPage.getRecord(lid);
		       DataPageInfo dpinfo = new DataPageInfo(atuple);
		       
		       answer += dpinfo.recct;
		     }
		   
		   // ASSERTIONS: no more record
	           // - we have read all datapage records on
	           //   the current directory page.
		   
		   nextDirPageId = currentDirPage.getNextPage();
		   unpinPage(currentDirPageId, false /*undirty*/);
		   currentDirPageId.pid = nextDirPageId.pid;
		}
	      
	      // ASSERTIONS:
	      // - if error, exceptions
	      // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
	      // - if not yet end of heapfile: currentDirPageId valid
	      
	      
	      return answer;
	    } // end of getRecCnt
	  
	  /** Insert record into file, return its Rid.
	   *
	   * @param recPtr pointer of the record
	   * @param recLen the length of the record
	   *
	   * @exception InvalidSlotNumberException invalid slot number
	   * @exception InvalidLabelSizeException invalid tuple size
	   * @exception SpaceNotAvailableException no space left
	   * @exception HFException heapfile exception
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception IOException I/O errors
	   *
	   * @return the rid of the record
	   */
	  public LID insertRecord(byte[] recPtr) 
	    throws InvalidSlotNumberException,  
		   InvalidLabelSizeException,
		   SpaceNotAvailableException,
		   LHFException,
		   LHFBufMgrException,
		   LHFDiskMgrException,
		   IOException
	    {
	      int dpinfoLen = 0;	
	      int recLen = recPtr.length;
	      boolean found;
	      LID currentDataPageRid = new LID();
	      Page pageinbuffer = new Page();
	      LHFPage currentDirPage = new LHFPage();
	      LHFPage currentDataPage = new LHFPage();
	      
	      LHFPage nextDirPage = new LHFPage(); 
	      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
	      PageId nextDirPageId = new PageId();  // OK
	      
	      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
	      
	      found = false;
	      Label atuple;
	      DataPageInfo dpinfo = new DataPageInfo();
	      while (found == false)
		{ //Start While01
		  // look for suitable dpinfo-struct
		  for (currentDataPageRid = currentDirPage.firstRecord();
		       currentDataPageRid != null;
		       currentDataPageRid = 
			 currentDirPage.nextRecord(currentDataPageRid))
		    {
		      atuple = currentDirPage.getRecord(currentDataPageRid);
		      
		      dpinfo = new DataPageInfo(atuple);
		      
		      // need check the record length == DataPageInfo'slength
		      
		       if(recLen <= dpinfo.availspace)
			 {
			   found = true;
			   break;
			 }  
		    }
		  
		  // two cases:
		  // (1) found == true:
		  //     currentDirPage has a datapagerecord which can accomodate
		  //     the record which we have to insert
		  // (2) found == false:
		  //     there is no datapagerecord on the current directory page
		  //     whose corresponding datapage has enough space free
		  //     several subcases: see below
		  if(found == false)
		    { //Start IF01
		      // case (2)
		      
		      //System.out.println("no datapagerecord on the current directory is OK");
		      //System.out.println("dirpage availspace "+currentDirPage.available_space());
		      
		      // on the current directory page is no datapagerecord which has
		      // enough free space
		      //
		      // two cases:
		      //
		      // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
		      //         if there is enough space on the current directory page
		      //         to accomodate a new datapagerecord (type DataPageInfo),
		      //         then insert a new DataPageInfo on the current directory
		      //         page
		      // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
		      //         look at the next directory page, if necessary, create it.
		      
		      if(currentDirPage.available_space() >= dpinfo.size)
			{ 
			  //Start IF02
			  // case (2.1) : add a new data page record into the
			  //              current directory page
			  currentDataPage = _newDatapage(dpinfo); 
			  // currentDataPage is pinned! and dpinfo->pageId is also locked
			  // in the exclusive mode  
			  
			  // didn't check if currentDataPage==NULL, auto exception
			  
			  
			  // currentDataPage is pinned: insert its record
			  // calling a HFPage function
			  
			  
			  
			  atuple = dpinfo.convertToLabel();
			  
			  byte [] tmpData = atuple.getLabelByteArray();
			  currentDataPageRid = currentDirPage.insertRecord(tmpData);
			  
			  LID tmprid = currentDirPage.firstRecord();
			  
			  
			  // need catch error here!
			  if(currentDataPageRid == null)
			    throw new LHFException(null, "no space to insert rec.");  
			  
			  // end the loop, because a new datapage with its record
			  // in the current directorypage was created and inserted into
			  // the heapfile; the new datapage has enough space for the
			  // record which the user wants to insert
			  
			  found = true;
			  
			} //end of IF02
		      else
			{  //Start else 02
			  // case (2.2)
			  nextDirPageId = currentDirPage.getNextPage();
			  // two sub-cases:
			  //
			  // (2.2.1) nextDirPageId != INVALID_PAGE:
			  //         get the next directory page from the buffer manager
			  //         and do another look
			  // (2.2.2) nextDirPageId == INVALID_PAGE:
			  //         append a new directory page at the end of the current
			  //         page and then do another loop
			    
			  if (nextDirPageId.pid != INVALID_PAGE) 
			    { //Start IF03
			      // case (2.2.1): there is another directory page:
			      unpinPage(currentDirPageId, false);
			      
			      currentDirPageId.pid = nextDirPageId.pid;
			      
			      pinPage(currentDirPageId,
							    currentDirPage, false);
			      
			      
			      
			      // now go back to the beginning of the outer while-loop and
			      // search on the current directory page for a suitable datapage
			    } //End of IF03
			  else
			    {  //Start Else03
			      // case (2.2): append a new directory page after currentDirPage
			      //             since it is the last directory page
			      nextDirPageId = newPage(pageinbuffer, 1);
			      // need check error!
			      if(nextDirPageId == null)
				throw new LHFException(null, "can't create new page");
			      
			      // initialize new directory page
			      nextDirPage.init(nextDirPageId, pageinbuffer);
			      PageId temppid = new PageId(INVALID_PAGE);
			      nextDirPage.setNextPage(temppid);
			      nextDirPage.setPrevPage(currentDirPageId);
			      
			      // update current directory page and unpin it
			      // currentDirPage is already locked in the Exclusive mode
			      currentDirPage.setNextPage(nextDirPageId);
			      unpinPage(currentDirPageId, true/*dirty*/);
			      
			      currentDirPageId.pid = nextDirPageId.pid;
			      currentDirPage = new LHFPage(nextDirPage);
			      
			      // remark that MINIBASE_BM->newPage already
			      // pinned the new directory page!
			      // Now back to the beginning of the while-loop, using the
			      // newly created directory page.
			      
			    } //End of else03
			} // End of else02
		      // ASSERTIONS:
		      // - if found == true: search will end and see assertions below
		      // - if found == false: currentDirPage, currentDirPageId
		      //   valid and pinned
		      
		    }//end IF01
		  else
		    { //Start else01
		      // found == true:
		      // we have found a datapage with enough space,
		      // but we have not yet pinned the datapage:
		      
		      // ASSERTIONS:
		      // - dpinfo valid
		      
		      // System.out.println("find the dirpagerecord on current page");
		      
		      pinPage(dpinfo.pageId, currentDataPage, false);
		      //currentDataPage.openHFpage(pageinbuffer);
		      
		      
		    }//End else01
		} //end of While01
	      
	      // ASSERTIONS:
	      // - currentDirPageId, currentDirPage valid and pinned
	      // - dpinfo.pageId, currentDataPageRid valid
	      // - currentDataPage is pinned!
	      
	      if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
		throw new LHFException(null, "invalid PageId");
	      
	      if (!(currentDataPage.available_space() >= recLen))
		throw new SpaceNotAvailableException(null, "no available space");
	      
	      if (currentDataPage == null)
		throw new LHFException(null, "can't find Data page");
	      
	      
	      LID lid;
	      lid = currentDataPage.insertRecord(recPtr);
	      
	      dpinfo.recct++;
	      dpinfo.availspace = currentDataPage.available_space();
	      
	      
	      unpinPage(dpinfo.pageId, true /* = DIRTY */);
	      
	      // DataPage is now released
	      atuple = currentDirPage.returnRecord(currentDataPageRid);
	      DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
	      
	      
	      dpinfo_ondirpage.availspace = dpinfo.availspace;
	      dpinfo_ondirpage.recct = dpinfo.recct;
	      dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
	      dpinfo_ondirpage.flushToLabel();
	      
	      
	      unpinPage(currentDirPageId, true /* = DIRTY */);
	      
	      
	      return lid;
	      
	    }
	  
	  /** Delete record from file with given rid.
	   *
	   * @exception InvalidSlotNumberException invalid slot number
	   * @exception InvalidLabelSizeException invalid tuple size
	   * @exception HFException heapfile exception
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception Exception other exception
	   *
	   * @return true record deleted  false:record not found
	   */
	  public boolean deleteRecord(LID lid)  
	    throws InvalidSlotNumberException, 
		   InvalidLabelSizeException, 
		   LHFException, 
		   LHFBufMgrException,
		   LHFDiskMgrException,
		   Exception
	  
	    {
	      boolean status;
	      LHFPage currentDirPage = new LHFPage();
	      PageId currentDirPageId = new PageId();
	      LHFPage currentDataPage = new LHFPage();
	      PageId currentDataPageId = new PageId();
	      LID currentDataPageRid = new LID();
	      
	      status = _findDataPage(lid,
				     currentDirPageId, currentDirPage, 
				     currentDataPageId, currentDataPage,
				     currentDataPageRid);
	      
	      if(status != true) return status;	// record not found
	      
	      // ASSERTIONS:
	      // - currentDirPage, currentDirPageId valid and pinned
	      // - currentDataPage, currentDataPageid valid and pinned
	      
	      // get datapageinfo from the current directory page:
	      Label atuple;	
	      
	      atuple = currentDirPage.returnRecord(currentDataPageRid);
	      DataPageInfo pdpinfo = new DataPageInfo(atuple);
	      
	      // delete the record on the datapage
	      currentDataPage.deleteRecord(lid);
	      
	      pdpinfo.recct--;
	      pdpinfo.flushToLabel();	//Write to the buffer pool
	      if (pdpinfo.recct >= 1) 
		{
		  // more records remain on datapage so it still hangs around.  
		  // we just need to modify its directory entry
		  
		  pdpinfo.availspace = currentDataPage.available_space();
		  pdpinfo.flushToLabel();
		  unpinPage(currentDataPageId, true /* = DIRTY*/);
		  
		  unpinPage(currentDirPageId, true /* = DIRTY */);
		  
		  
		}
	      else
		{
		  // the record is already deleted:
		  // we're removing the last record on datapage so free datapage
		  // also, free the directory page if 
		  //   a) it's not the first directory page, and 
		  //   b) we've removed the last DataPageInfo record on it.
		  
		  // delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
		  unpinPage(currentDataPageId, false /*undirty*/);
		  
		  freePage(currentDataPageId);
		  
		  // delete corresponding DataPageInfo-entry on the directory page:
		  // currentDataPageRid points to datapage (from for loop above)
		  
		  currentDirPage.deleteRecord(currentDataPageRid);
		  
		  
		  // ASSERTIONS:
		  // - currentDataPage, currentDataPageId invalid
		  // - empty datapage unpinned and deleted
		  
		  // now check whether the directory page is empty:
		  
		  currentDataPageRid = currentDirPage.firstRecord();
		  
		  // st == OK: we still found a datapageinfo record on this directory page
		  PageId pageId;
		  pageId = currentDirPage.getPrevPage();
		  if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE))
		    {
		      // the directory-page is not the first directory page and it is empty:
		      // delete it
		      
		      // point previous page around deleted page:
		      
		      LHFPage prevDirPage = new LHFPage();
		      pinPage(pageId, prevDirPage, false);

		      pageId = currentDirPage.getNextPage();
		      prevDirPage.setNextPage(pageId);
		      pageId = currentDirPage.getPrevPage();
		      unpinPage(pageId, true /* = DIRTY */);
		      
		      
		      // set prevPage-pointer of next Page
		      pageId = currentDirPage.getNextPage();
		      if(pageId.pid != INVALID_PAGE)
			{
			  LHFPage nextDirPage = new LHFPage();
			  pageId = currentDirPage.getNextPage();
			  pinPage(pageId, nextDirPage, false);
			  
			  //nextDirPage.openHFpage(apage);
			  
			  pageId = currentDirPage.getPrevPage();
			  nextDirPage.setPrevPage(pageId);
			  pageId = currentDirPage.getNextPage();
			  unpinPage(pageId, true /* = DIRTY */);
			  
			}
		      
		      // delete empty directory page: (automatically unpinned?)
		      unpinPage(currentDirPageId, false/*undirty*/);
		      freePage(currentDirPageId);
		      
		      
		    }
		  else
		    {
		      // either (the directory page has at least one more datapagerecord
		      // entry) or (it is the first directory page):
		      // in both cases we do not delete it, but we have to unpin it:
		      
		      unpinPage(currentDirPageId, true /* == DIRTY */);
		      
		      
		    }
		}
	      return true;
	    }
	  
	  
	  /** Updates the specified record in the heapfile.
	   * @param rid: the record which needs update
	   * @param newtuple: the new content of the record
	   *
	   * @exception InvalidSlotNumberException invalid slot number
	   * @exception InvalidUpdateException invalid update on record
	   * @exception InvalidLabelSizeException invalid tuple size
	   * @exception HFException heapfile exception
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception Exception other exception
	   * @return ture:update success   false: can't find the record
	   */
	  public boolean updateRecord(LID lid, Label newtuple) 
	    throws InvalidSlotNumberException, 
		   InvalidUpdateException, 
		   InvalidLabelSizeException,
		   LHFException, 
		   LHFDiskMgrException,
		   LHFBufMgrException,
		   Exception
	    {
	      boolean status;
	      LHFPage dirPage = new LHFPage();
	      PageId currentDirPageId = new PageId();
	      LHFPage dataPage = new LHFPage();
	      PageId currentDataPageId = new PageId();
	      LID currentDataPageRid = new LID();
	      
	      status = _findDataPage(lid,
				     currentDirPageId, dirPage, 
				     currentDataPageId, dataPage,
				     currentDataPageRid);
	      
	      if(status != true) return status;	// record not found
	      Label atuple = new Label();
	      atuple = dataPage.returnRecord(lid);
	      
	      // Assume update a record with a record whose length is equal to
	      // the original record
	      
	      if(newtuple.getLength() != atuple.getLength())
		{
		  unpinPage(currentDataPageId, false /*undirty*/);
		  unpinPage(currentDirPageId, false /*undirty*/);
		  
		  throw new InvalidUpdateException(null, "invalid record update");
		  
		}

	      // new copy of this record fits in old space;
	      atuple.LabelCopy(newtuple);
	      unpinPage(currentDataPageId, true /* = DIRTY */);
	      
	      unpinPage(currentDirPageId, false /*undirty*/);
	      
	      return true;
	    }
	  
	  
	  /** Read record from file, returning pointer and length.
	   * @param rid Record ID
	   *
	   * @exception InvalidSlotNumberException invalid slot number
	   * @exception InvalidLabelSizeException invalid tuple size
	   * @exception SpaceNotAvailableException no space left
	   * @exception HFException heapfile exception
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception Exception other exception
	   *
	   * @return a Label. if Label==null, no more tuple
	   */
	  public  Label getRecord(LID lid) 
	    throws InvalidSlotNumberException, 
		   InvalidLabelSizeException, 
		   LHFException, 
		   LHFDiskMgrException,
		   LHFBufMgrException,
		   Exception
	    {
	      boolean status;
	      LHFPage dirPage = new LHFPage();
	      PageId currentDirPageId = new PageId();
	      LHFPage dataPage = new LHFPage();
	      PageId currentDataPageId = new PageId();
	      LID currentDataPageRid = new LID();
	      
	      status = _findDataPage(lid,
				     currentDirPageId, dirPage, 
				     currentDataPageId, dataPage,
				     currentDataPageRid);
	      
	      if(status != true) return null; // record not found 
	      
	      Label atuple = new Label();
	      atuple = dataPage.getRecord(lid);
	      
	      /*
	       * getRecord has copied the contents of rid into recPtr and fixed up
	       * recLen also.  We simply have to unpin dirpage and datapage which
	       * were originally pinned by _findDataPage.
	       */    
	      
	      unpinPage(currentDataPageId,false /*undirty*/);
	      
	      unpinPage(currentDirPageId,false /*undirty*/);
	      
	      
	      return  atuple;  //(true?)OK, but the caller need check if atuple==NULL
	      
	    }
	  
	  
	  /** Initiate a sequential scan.
	   * @exception InvalidLabelSizeException Invalid tuple size
	   * @exception IOException I/O errors
	   *
	   */
	  public LScan openScan() 
	    throws InvalidLabelSizeException,
		   IOException
	    {
	      LScan newscan = new LScan(this);
	      return newscan;
	    }
	  
	  
	  /** Delete the file from the database.
	   *
	   * @exception InvalidSlotNumberException invalid slot number
	   * @exception InvalidLabelSizeException invalid tuple size
	   * @exception FileAlreadyDeletedException file is deleted already
	   * @exception HFBufMgrException exception thrown from bufmgr layer
	   * @exception HFDiskMgrException exception thrown from diskmgr layer
	   * @exception IOException I/O errors
	   */
	  public void deleteFile()  
	    throws InvalidSlotNumberException, 
		   FileAlreadyDeletedException, 
		   InvalidLabelSizeException, 
		   LHFBufMgrException,
		   LHFDiskMgrException,
		   IOException
	    {
	      if(_file_deleted ) 
	   	throw new FileAlreadyDeletedException(null, "file alread deleted");
	      
	      
	      // Mark the deleted flag (even if it doesn't get all the way done).
	      _file_deleted = true;
	      
	      // Deallocate all data pages
	      PageId currentDirPageId = new PageId();
	      currentDirPageId.pid = _firstDirPageId.pid;
	      PageId nextDirPageId = new PageId();
	      nextDirPageId.pid = 0;
	      Page pageinbuffer = new Page();
	      LHFPage currentDirPage =  new LHFPage();
	      Label atuple;
	      
	      pinPage(currentDirPageId, currentDirPage, false);
	      //currentDirPage.openHFpage(pageinbuffer);
	      
	      LID lid = new LID();
	      while(currentDirPageId.pid != INVALID_PAGE)
		{      
		  for(lid = currentDirPage.firstRecord();
		      lid != null;
		      lid = currentDirPage.nextRecord(lid))
		    {
		      atuple = currentDirPage.getRecord(lid);
		      DataPageInfo dpinfo = new DataPageInfo( atuple);
		      //int dpinfoLen = arecord.length;
		      
		      freePage(dpinfo.pageId);
		      
		    }
		  // ASSERTIONS:
		  // - we have freePage()'d all data pages referenced by
		  // the current directory page.
		  
		  nextDirPageId = currentDirPage.getNextPage();
		  freePage(currentDirPageId);
		  
		  currentDirPageId.pid = nextDirPageId.pid;
		  if (nextDirPageId.pid != INVALID_PAGE) 
		    {
		      pinPage(currentDirPageId, currentDirPage, false);
		      //currentDirPage.openHFpage(pageinbuffer);
		    }
		}
	      
	      delete_file_entry( _fileName );
	    }
	  
	  /**
	   * short cut to access the pinPage function in bufmgr package.
	   * @see bufmgr.pinPage
	   */
	  private void pinPage(PageId pageno, Page page, boolean emptyPage)
	    throws LHFBufMgrException {
	    
	    try {
	      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
	    }
	    catch (Exception e) {
	      System.out.println(e);
	      throw new LHFBufMgrException(e,"Heapfile.java: pinPage() failed");
	    }
	    
	  } // end of pinPage

	  /**
	   * short cut to access the unpinPage function in bufmgr package.
	   * @see bufmgr.unpinPage
	   */
	  private void unpinPage(PageId pageno, boolean dirty)
	    throws LHFBufMgrException {

	    try {
	    	
	      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
	      
	    }
	    catch (Exception e) {
	      throw new LHFBufMgrException(e,"Heapfile.java: unpinPage() failed");
	    }

	  } // end of unpinPage

	  private void freePage(PageId pageno)
	    throws LHFBufMgrException {

	    try {
	      SystemDefs.JavabaseBM.freePage(pageno);
	    }
	    catch (Exception e) {
	      throw new LHFBufMgrException(e,"Heapfile.java: freePage() failed");
	    }

	  } // end of freePage

	  private PageId newPage(Page page, int num)
	    throws LHFBufMgrException {

	    PageId tmpId = new PageId();

	    try {
	      tmpId = SystemDefs.JavabaseBM.newPage(page,num);
	    }
	    catch (Exception e) {
	      throw new LHFBufMgrException(e,"Heapfile.java: newPage() failed");
	    }

	    return tmpId;

	  } // end of newPage

	  private PageId get_file_entry(String filename)
	    throws LHFDiskMgrException {

	    PageId tmpId = new PageId();

	    try {
	      tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
	    }
	    catch (Exception e) {
	      throw new LHFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
	    }

	    return tmpId;

	  } // end of get_file_entry

	  private void add_file_entry(String filename, PageId pageno)
	    throws LHFDiskMgrException {

	    try {
	      SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
	    }
	    catch (Exception e) {
	      throw new LHFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
	    }

	  } // end of add_file_entry

	  private void delete_file_entry(String filename)
	    throws LHFDiskMgrException {

	    try {
	      SystemDefs.JavabaseDB.delete_file_entry(filename);
	    }
	    catch (Exception e) {
	      throw new LHFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
	    }

	  } // end of delete_file_entry


	  
	}// End of LabelHeapFile 
