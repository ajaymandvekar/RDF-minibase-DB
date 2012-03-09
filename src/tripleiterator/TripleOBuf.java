package tripleiterator;

import tripleheap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;

import java.io.*;

/**
 *TripleOBuf::Put takes triples and stores them on the buffer pages that
 *were passed to TripleOBuf::init.  TripleOBuf::flush inserts them enmass into
 *a temporary Triple HeapFile.
 */
public class TripleOBuf implements GlobalConst
{
  
  /**
   *fault constructor
   * no args -- use init to initialize
   */
  public TripleOBuf(){}     

  /**
   * Triple_O_buf is an output buffer. It takes as input:
   *@param bufs    temporary buffer to pages.(EACH ELEMENT IS A SINGLE BUFFER PAGE).
   *@param n_pages the number of pages
   *@param temp_fd fd of a TripleHeapFile
   *@param buffer  true => it is used as a buffer => if it is flushed, print
   *                      a nasty message. it is false by default.
  */
  public void init(byte[][] bufs, int n_pages, TripleHeapfile temp_fd, boolean buffer)
  {
	  _bufs    = bufs;
	  _n_pages = n_pages;
	  t_size   = RDF_TRIPLE_SIZE;
	  _temp_fd = temp_fd;

	  dirty       = false;
	  t_per_pg    = MINIBASE_PAGESIZE / t_size;
	  t_in_buf    = n_pages * t_per_pg;
	  t_wr_to_pg  = 0;
	  t_wr_to_buf = 0;
	  t_written   = 0L;
	  curr_page   = 0;
	  buffer_only = buffer;
  }
  
  /**
   * Writes a triple to the output buffer
   *@param buf the triple written to buffer
   *@return the position of tuple which is in buffer
   *@exception IOException  some I/O fault
   *@exception Exception other exceptions
   */
  public Triple Put(Triple buf)
  throws IOException,Exception
  {
	  byte[] copybuf;
	  copybuf = buf.getTripleByteArray();
	  System.arraycopy(copybuf,0,_bufs[curr_page],t_wr_to_pg*t_size,t_size); 
	  Triple triple_ptr = new Triple(_bufs[curr_page] , t_wr_to_pg * t_size,t_size);

	  t_written++; t_wr_to_pg++; t_wr_to_buf++; dirty = true;

	  if (t_wr_to_buf == t_in_buf)                // Buffer full?
	  {
		  flush();                                // Flush it
		  t_wr_to_pg = 0; t_wr_to_buf = 0;        // Initialize page info
		  curr_page  = 0;
	  }
	  else if (t_wr_to_pg == t_per_pg)
	  {
		  t_wr_to_pg = 0;
		  curr_page++;
	  }

	  return triple_ptr;
  }
  
  /**
   * returns the # of triples written.
   *@return the numbers of triples written
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public long flush() throws IOException, Exception
  {
	  int count;
	  int bytes_written = 0;
	  byte[] tempbuf = new byte[t_size];
	  if (buffer_only == true)
	  {
		  System.out.println("Stupid error - but no error protocol");
	  }

	  if (dirty)
	  {
		  for (count = 0; count <= curr_page; count++)
		  {
			  TID tid = null;
			  // Will have to go thru entire buffer writing tuples to disk

			  if (count == curr_page)
			  {
				  for (int i = 0; i < t_wr_to_pg; i++)
				  {
					  System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
					  try {
						  tid =  _temp_fd.insertTriple(tempbuf);
					  }
					  catch (Exception e)
					  {
						  throw e;
					  }
				  }
			  }
			  else
			  {
				  for (int i = 0; i < t_per_pg; i++)
				  {       
					  System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
					  try 
					  {
						  tid =  _temp_fd.insertTriple(tempbuf);
					  }
					  catch (Exception e)
					  {
						  throw e;
					  }
				  }
			  }
			 /*if(tid != null)
			  {
				  if(tid.pageNo.pid>0)
				  {
					  SystemDefs.JavabaseBM.flushPage(tid.pageNo);
				  }
			  }*/
		  }
		  dirty = false;
	  }

	  return t_written;
  }
  
  private boolean dirty;           	// Does this buffer contain dirty pages?
  private  int  t_per_pg,               // # of triples that fit in 1 page
                t_in_buf;               // # of triples that fit in the buffer
  private  int  t_wr_to_pg,             // # of triples written to current page
    		t_wr_to_buf;            // # of triples written to buffer.
  private  int  curr_page;              // Current page being written to.
  private  byte[][]_bufs;               // Array of pointers to buffer pages.
  private  int  _n_pages;               // number of pages in array
  private  int  t_size;                 // Size of a triple
  private  long t_written;              // # of triples written so far.
  private  int  TEST_temp_fd;           // fd of a temporary file
  private  TripleHeapfile _temp_fd;
  private  boolean buffer_only;
}



