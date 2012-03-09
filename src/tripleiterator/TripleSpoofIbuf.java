package tripleiterator;

import tripleheap.*;          
import global.*;
import diskmgr.*;
import bufmgr.*;

import java.io.*;

public class TripleSpoofIbuf implements GlobalConst  {

	/**
	 *constructor, use the init to initialize
	 */
	public void TripleSpoofIbuf()
	{

		hf_scan = null;
	}


	/**
	 *Initialize some necessary inormation, call Iobuf to create the
	 *object, and call init to finish intantiation
	 *@param bufs[][] the I/O buffer
	 *@param n_pages the numbers of page of this buffer
	 *@param fd the reference to an TripleHeapfile
	 *@param Ntriples the tuple numbers of the page
	 *@exception IOException some I/O fault
	 *@exception Exception other exceptions
	 */
	public  void init(TripleHeapfile fd, byte bufs[][], int n_pages, int Ntriples)
		throws IOException,Exception
		{
			_fd       = fd;       _bufs        = bufs;
			_n_pages  = n_pages;  t_size       = RDF_TRIPLE_SIZE;

			t_proc    = 0;        t_in_buf     = 0;
			tot_t_proc= 0;
			curr_page = 0;        t_rd_from_pg = 0;
			done      = false;    t_per_pg     = MINIBASE_PAGESIZE / t_size;

			n_triples = Ntriples;

			// open a scan
			if (hf_scan != null)  hf_scan = null;

			try 
			{
				hf_scan = new TScan(fd); 
			}
			catch(Exception e)
			{
				throw e;
			}
		}

	/** 
	 *get a triple from current buffer,pass reference buf to this method
	 *usage:temp_triple = triple.Get(buf); 
	 *@param buf write the result to buf
	 *@return the result triple
	 *@exception IOException some I/O fault
	 *@exception Exception other exceptions
	 */
	public  Triple Get(Triple  buf)throws IOException, Exception
	{
		if (tot_t_proc == n_triples) done = true;

		if (done == true){buf = null; return null;}
		if (t_proc == t_in_buf)
		{
			try 
			{
				t_in_buf = readin();
			}
			catch (Exception e)
			{
				throw e;
			}
			curr_page = 0; t_rd_from_pg = 0; t_proc = 0;
		}

		if (t_in_buf == 0)                        // No triples read in?
		{
			done = true; buf = null;return null;
		}

		buf.tripleSet(_bufs[curr_page],t_rd_from_pg*t_size,t_size); 
		tot_t_proc++;

		// Setup for next read
		t_rd_from_pg++; t_proc++;
		if (t_rd_from_pg == t_per_pg)
		{
			t_rd_from_pg = 0; curr_page++;
		}
		return buf;
	}


	/**
	 *@return if the buffer is empty,return true. otherwise false
	 */
	public  boolean empty()
	{
		if (tot_t_proc == n_triples) done = true;
		return done;
	}

	/**
	 *
	 *@return the numbers of triples in the buffer
	 *@exception IOException some I/O fault
	 *@exception InvalidTripleSizeException Triple Heapfile error
	 */
	private int readin()throws IOException,InvalidTripleSizeException
	{
		int t_read = 0, tot_read = 0;
		Triple t = new Triple ();
		byte[] t_copy;

		curr_page = 0;
		while (curr_page < _n_pages)
		{
			while (t_read < t_per_pg)
			{
				TID tid =new TID();
				try 
				{
					if ( (t = hf_scan.getNext(tid)) == null) return tot_read;
					t_copy = t.getTripleByteArray();
					System.arraycopy(t_copy,0,_bufs[curr_page],t_read*t_size,t_size); 
				}
				catch (Exception e) 
				{
					System.err.println (""+e);
				}
				t_read++; tot_read++;
			} 
			t_read     = 0;
			curr_page++;
		}
		return tot_read;
	}


	private  byte[][] _bufs;

	private  int   TEST_fd;

	private  TripleHeapfile _fd;
	private  TScan hf_scan;
	private  int    _n_pages;
	private  int    t_size;

	private  int    t_proc, t_in_buf;
	private  int    tot_t_proc;
	private  int    t_rd_from_pg, curr_page;
	private  int    t_per_pg;
	private  boolean   done;
	private  int    n_triples;
}
