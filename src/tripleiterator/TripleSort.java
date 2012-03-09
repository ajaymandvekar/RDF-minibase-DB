package tripleiterator;

import java.io.*; 

import global.*;
import bufmgr.*;
import diskmgr.*;
import tripleheap.*;
import index.*;
import chainexception.*;

/**
 * The TripleSort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class TripleSort extends TripleIterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 10;
  
  private TScan       _am;
  private TripleOrder order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         Nruns;
  private int         max_elems_in_heap;
  private int         triple_size;
  
  private TriplepnodeSplayPQ Q;
  private TripleHeapfile[]   temp_files; 
  private int          n_tempfiles;
  private Triple       output_triple;  
  private int[]        n_triples;
  private int          n_runs;
  private Triple       op_buf;
  private TripleOBuf   o_buf;
  private TripleSpoofIbuf[]  i_buf;
  private PageId[]     bufs_pids;
  private boolean useBM = true; // flag for whether to use buffer manager
  
  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get 
   * the minimum of all runs.
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to sort in two passes (a subclass of TripleSortException).
   * @exception TripleSortException something went wrong in the lower layer. 
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int n_R_runs)
  throws IOException, 
  LowMemException, 
  TripleSortException,
  Exception
  {
	  // don't know what will happen if n_R_runs > _n_pages
	  if (n_R_runs > _n_pages) 
		  throw new LowMemException("Triple Sort.java: Not enough memory to sort in two passes."); 

	  int i;
	  Triplepnode cur_node;  // need pq_defs.java

	  i_buf = new TripleSpoofIbuf[n_R_runs];   // need io_bufs.java
	  for (int j=0; j<n_R_runs; j++) i_buf[j] = new TripleSpoofIbuf();

	  // construct the lists, ignore TEST for now
	  // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

	  for (i=0; i<n_R_runs; i++) 
	  {
		  byte[][] apage = new byte[1][];
		  apage[0] = bufs[i];

		  // need iobufs.java
		  i_buf[i].init(temp_files[i], apage, 1, n_triples[i]);

		  cur_node = new Triplepnode();
		  cur_node.run_num = i;

		  // may need change depending on whether Get() returns the original
		  // or make a copy of the tuple, need io_bufs.java ???
		  Triple temp_triple = new Triple();

		  temp_triple = i_buf[i].Get(temp_triple);  // need io_bufs.java

		  if (temp_triple != null) 
		  {
			  /*
			     System.out.print("Get tuple from run " + i);
			     temp_tuple.print(_in);
			   */
			  cur_node.triple = temp_triple; // no copy needed
			  try 
			  {
				  Q.Tripleenq(cur_node);
			  }
			  catch (TripleUtilsException e) 
			  {
				  throw new TripleSortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
			  }
		  }
	  }
	  return; 
  }
  
  private Triple createDummyLastElement()
  { 
    PageId pageno = new PageId(-1);
        
    LID lid = new LID(pageno,-1);
    Triple triple = new Triple();
    try {
		triple.setSubjectID(lid.returnEID());
		triple.setPredicateID(lid.returnPID());
	    triple.setObjectID(lid.returnEID());
	    triple.setConfidence(-1);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	
	return triple;
  }

  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception TripleSortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems) 
  throws IOException, 
  TripleSortException, 
  TripleUtilsException,
  UnknowAttrType,
  Exception
  {
    Triple triple; 
    Triplepnode cur_node;
    TriplepnodeSplayPQ Q1 = new TriplepnodeSplayPQ(order);
    TriplepnodeSplayPQ Q2 = new TriplepnodeSplayPQ(order);
    TriplepnodeSplayPQ pcurr_Q = Q1;
    TriplepnodeSplayPQ pother_Q = Q2; 

    Triple lastElem = createDummyLastElement(); //Dummy last element

    int run_num = 0;  // keeps track of the number of runs

    // number of elements in Q
    //    int nelems_Q1 = 0;
    //    int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) 
    {
	   
	    try 
	    {
		    TID tid = new TID();
		    triple = _am.getNext(tid);  // according to TScan.java
	    } catch (Exception e) 
	    {
		    e.printStackTrace(); 
		    throw new TripleSortException(e, "Triple Sort.java: get_next() failed");
	    } 

	    if (triple == null) 
	    {
		    break;
	    }

	    cur_node = new Triplepnode();
	    cur_node.triple = new Triple(triple); // triple copy needed --  Bingjie 4/29/98 

	    pcurr_Q.Tripleenq(cur_node);
	    p_elems_curr_Q ++;
    }
    
    // now the queue is full, starting writing to file while keep trying
    // to add new triples to the queue. The ones that does not fit are put
    // on the other queue temperarily.
    while (true) 
    {
	    cur_node = pcurr_Q.Tripledeq();
	    if (cur_node == null) break; 
	    p_elems_curr_Q --;

	    comp_res = TripleUtils.CompareTripleWithTriple(order,cur_node.triple, lastElem);  // need triple_utils.java

	    if ((comp_res < 0)) 
	    {
		    // doesn't fit in current run, put into the other queue
		    try 
		    {
			    pother_Q.Tripleenq(cur_node);
		    }
		    catch (UnknowAttrType e) 
		    {
			    throw new TripleSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
		    }
		    p_elems_other_Q ++;
	    }
	    else 
	    {
		    // set lastElem to have the value of the current tuple,
		    // need tuple_utils.java
		    TripleUtils.SetValue(lastElem, cur_node.triple);
		    // write tuple to output file, need io_bufs.java, type cast???
		    //	System.out.println("Putting tuple into run " + (run_num + 1)); 
		    //	cur_node.tuple.print(_in);

		    o_buf.Put(cur_node.triple);
	    }

	    // check whether the other queue is full
	    if (p_elems_other_Q == max_elems) 
	    {
		    // close current run and start next run
		    n_triples[run_num] = (int) o_buf.flush();  // need io_bufs.java
		    run_num ++;

		    // check to see whether need to expand the array
		    if (run_num == n_tempfiles) 
		    {
			    TripleHeapfile[] temp1 = new TripleHeapfile[2*n_tempfiles];
			    for (int i=0; i<n_tempfiles; i++) 
			    {
				    temp1[i] = temp_files[i];
			    }

			    temp_files = temp1; 
			    n_tempfiles *= 2; 

			    int[] temp2 = new int[2*n_runs];
			    for(int j=0; j<n_runs; j++) 
			    {
				    temp2[j] = n_triples[j];
			    }
			    n_triples = temp2;
			    n_runs *=2; 
		    }

		    try 
		    {
			    temp_files[run_num] = new TripleHeapfile(null);
		    }
		    catch (Exception e) {
			    throw new TripleSortException(e, "Triple Sort.java: create Heapfile failed");
		    }

		    // need io_bufs.java
		    o_buf.init(bufs, _n_pages, temp_files[run_num], false);

		    lastElem = createDummyLastElement();

		    // switch the current heap and the other heap
		    TriplepnodeSplayPQ tempQ = pcurr_Q;
		    pcurr_Q = pother_Q;
		    pother_Q = tempQ;
		    int tempelems = p_elems_curr_Q;
		    p_elems_curr_Q = p_elems_other_Q;
		    p_elems_other_Q = tempelems;
	    }

	    // now check whether the current queue is empty
	    else if (p_elems_curr_Q == 0) 
	    {
		    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) 
		    {
			    try 
			    {
				    TID tid = new TID();
				    triple = _am.getNext(tid);  // according to Iterator.java
			    } catch (Exception e) {
				    throw new TripleSortException(e, "get_next() failed");
			    } 

			    if (triple == null) 
			    {
				    break;
			    }
			    cur_node = new Triplepnode();
			    cur_node.triple = new Triple(triple); // tuple copy needed --  Bingjie 4/29/98 

			    try 
			    {
				    pcurr_Q.Tripleenq(cur_node);
			    }
			    catch (UnknowAttrType e) 
			    {
				    throw new TripleSortException(e, "Triple Sort.java: UnknowAttrType caught from Q.enq()");
			    }
			    p_elems_curr_Q ++;
		    }
	    }

	    // Check if we are done
	    if (p_elems_curr_Q == 0) 
	    {
		    // current queue empty despite our attemps to fill in
		    // indicating no more tuples from input
		    if (p_elems_other_Q == 0) 
		    {
			    // other queue is also empty, no more tuples to write out, done
			    break; // of the while(true) loop
		    }
		    else 
                    {
			    // generate one more run for all tuples in the other queue
			    // close current run and start next run
			    n_triples[run_num] = (int) o_buf.flush();  // need io_bufs.java
			    run_num ++;

			    // check to see whether need to expand the array
			    if (run_num == n_tempfiles) 
			    {
				    TripleHeapfile[] temp1 = new TripleHeapfile[2*n_tempfiles];
				    for (int i=0; i<n_tempfiles; i++) 
				    {
					    temp1[i] = temp_files[i];
				    }
				    temp_files = temp1; 
				    n_tempfiles *= 2; 

				    int[] temp2 = new int[2*n_runs];
				    for(int j=0; j<n_runs; j++) 
				    {
					    temp2[j] = n_triples[j];
				    }

				    n_triples = temp2;
				    n_runs *=2; 
			    }

			    try 
			    {
				    temp_files[run_num] = new TripleHeapfile(null); 
			    }
			    catch (Exception e) {
				    throw new TripleSortException(e, "Sort.java: create Heapfile failed");
			    }

			    // need io_bufs.java
			    o_buf.init(bufs, _n_pages,  temp_files[run_num], false);

			    lastElem = createDummyLastElement();

			    // switch the current heap and the other heap
			    TriplepnodeSplayPQ tempQ = pcurr_Q;
			    pcurr_Q = pother_Q;
			    pother_Q = tempQ;
			    int tempelems = p_elems_curr_Q;
			    p_elems_curr_Q = p_elems_other_Q;
			    p_elems_other_Q = tempelems;
		    }
	    } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_triples[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }
  
  /**
   * Remove the minimum value among all the runs.
   * @return the minimum triple removed
   * @exception IOException from lower layers
   * @exception TripleSortException something went wrong in the lower layer. 
   */
  private Triple delete_min() 
  throws IOException, 
  TripleSortException,
  Exception
  {
	  Triplepnode cur_node;                // needs pq_defs.java  
	  Triple new_triple, old_triple;  

	  cur_node = Q.Tripledeq();
	  old_triple = cur_node.triple;
	  /*
	     System.out.print("Get ");
	     old_tuple.print(_in);
	   */
	  // we just removed one tuple from one run, now we need to put another
	  // tuple of the same run into the queue
	  if (i_buf[cur_node.run_num].empty() != true) 
	  { 
		  // run not exhausted 
		  new_triple = new Triple(); // need triple.java

		  new_triple = i_buf[cur_node.run_num].Get(new_triple);

		  if (new_triple != null) 
		  {
			  /*
			     System.out.print(" fill in from run " + cur_node.run_num);
			     new_tuple.print(_in);
			   */
			  cur_node.triple = new_triple;  // no copy needed -- I think Bingjie 4/22/98
			  try 
			  {
				  Q.Tripleenq(cur_node);
			  } catch (UnknowAttrType e) 
			  {
				  throw new TripleSortException(e, "TripleSort.java: UnknowAttrType caught from Q.enq()");
			  } catch (TripleUtilsException e) 
			  {
				  throw new TripleSortException(e, "TripleSort.java: TupleUtilsException caught from Q.enq()");
			  } 
		  }
		  else 
		  {
			  throw new TripleSortException("********** Wait a minute, I thought input is not empty ***************");
		  }

	  }

	  // changed to return Triple instead of return char array ????
	  return old_triple; 
  }
  
  /** 
   * Class constructor, take information about the tuples, and set up 
   * the sorting
   * @param am an Heap file scan iterator for accessing the triples
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception TripleSortException something went wrong in the lower layer. 
   */
  public TripleSort(TScan am, TripleOrder sort_order, int n_pages) 
  throws IOException,TripleSortException
  {
	  _am = am;
	  order = sort_order;
	  _n_pages = n_pages;

	  // this may need change, bufs ???  need io_bufs.java
	  // bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
	  bufs_pids = new PageId[_n_pages];
	  bufs = new byte[_n_pages][];

	  if (useBM) 
	  {
		  try 
		  {
			  get_buffer_pages(_n_pages, bufs_pids, bufs);
		  }
		  catch (Exception e) 
		  {
			  throw new TripleSortException(e, "Triple Sort.java: BUFmgr error");
		  }
	  }
	  else 
	  {
		  for (int k=0; k<_n_pages; k++) 
		  {
			  bufs[k] = new byte[MAX_SPACE];
		  }
	  }

	  first_time = true;

	  // as a heuristic, we set the number of runs to an arbitrary value
	  // of ARBIT_RUNS
	  temp_files = new TripleHeapfile[ARBIT_RUNS];
	  n_tempfiles = ARBIT_RUNS;
	  n_triples = new int[ARBIT_RUNS]; 
	  n_runs = ARBIT_RUNS;

	  try 
	  {
		  temp_files[0] = new TripleHeapfile(null);
	  }
	  catch (Exception e) 
	  {
		  throw new TripleSortException(e, "Triple Sort.java: TripleHeapfile error");
	  }

	  o_buf = new TripleOBuf();

	  o_buf.init(bufs, _n_pages, temp_files[0], false);
	  //    output_tuple = null;

	  max_elems_in_heap = 200;

	  Q = new TriplepnodeSplayPQ(order);
	  
	  op_buf = new Triple();
  }
  
  /**
   * Returns the next triple in sorted order.
   * Note: You need to copy out the content of the triple, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next triple, null if all triples exhausted
   * @exception IOException from lower layers
   * @exception TripleSortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception LowMemException memory low exception
   * @exception Exception other exceptions
   */
  public Triple get_next() 
  throws IOException,
  TripleSortException, 
  LowMemException, 
  Exception
  {
	  if (first_time) 
	  {
		  // first get_next call to the sort routine
		  first_time = false;

		  // generate runs
		  Nruns = generate_runs(max_elems_in_heap);
		        //System.out.println("Generated " + Nruns + " runs");

		  // setup state to perform merge of runs. 
		  // Open input buffers for all the input file
		  setup_for_merge(Nruns);
	  }

	  if (Q.empty()) 
	  {  
		  // no more tuples availble
		  return null;
	  }

	  output_triple = delete_min();
	  if (output_triple != null)
	  {
		  op_buf.tripleCopy(output_triple);
		  return op_buf; 
	  }
	  else 
		  return null; 
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception TripleSortException something went wrong in the lower layer. 
   */
  public void close() throws 
  TripleSortException,
  IOException
  {
	  // clean up
	  if (!closeFlag) 
	  {
		  try 
		  {
			  _am.closescan();
		  }
		  catch (Exception e) 
		  {
			  throw new TripleSortException(e, "TripleSort.java: error in closing Tscan.");
		  }

		  if (useBM) 
		  {
			  try 
			  {
				  free_buffer_pages(_n_pages, bufs_pids);
			  } 
			  catch (Exception e) 
			  {
				  throw new TripleSortException(e, "TripleSort.java: BUFmgr error");
			  }
			  for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
		  }

		  for (int i = 0; i<temp_files.length; i++) 
		  {
			  if (temp_files[i] != null) 
			  {
				  try 
				  {
					  temp_files[i].deleteFile();
				  }
				  catch (Exception e) 
				  {
					  throw new TripleSortException(e, "Triple Sort.java: Triple Heapfile error");
				  }
				  temp_files[i] = null; 
			  }
		  }
		  closeFlag = true;
	  } 
  } 

}


