package basicpatterniterator;
   

import heap.*;
import tripleheap.*;
import labelheap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.util.ArrayList;
import java.io.*;

import javax.lang.model.type.ArrayType;

import iterator.*;
import basicpattern.*;


public class BP_Triple_Join  extends BasicPatternIterator
{

	int amt_of_mem;
	int num_left_nodes;
	BasicPatternIterator left_iter;
	int BPJoinNodePosition;
	int JoinOnSubjectorObject;
	String RightSubjectFilter;
	String RightPredicateFilter;
	String RightObjectFilter;
	double RightConfidenceFilter;
	int [] LeftOutNodePosition;
	int OutputRightSubject;
	int OutputRightObject;
	private   boolean    done;         // Is the join 
	private boolean	  get_from_outer;                 // if TRUE, a tuple is got from outer
	//private   TScan      inner; //XXX
	private Stream inner;
	private   BasicPattern  outer_tuple;
	private Triple inner_tuple;
	TripleHeapfile Triple_HF;

	// joins the basic pattern specified by the left_iter 
	// with the triples in the RDF data store (entity HF)	
	public BP_Triple_Join(int amt_of_mem, 
			int num_left_nodes,
			BasicPatternIterator left_iter,
			int BPJoinNodePosition,
			int JoinOnSubjectorObject,
			String RightSubjectFilter,
			String RightPredicateFilter,
			String RightObjectFilter,
			double RightConfidenceFilter,
			int [] LeftOutNodePosition,
			int OutputRightSubject,	
			int OutputRightObject)
	{
		this.amt_of_mem = amt_of_mem;
		this.num_left_nodes = num_left_nodes;
		this.left_iter = left_iter;
		this.BPJoinNodePosition = BPJoinNodePosition;
		this.JoinOnSubjectorObject = JoinOnSubjectorObject;
		this.RightSubjectFilter = new String(RightSubjectFilter);
		this.RightObjectFilter = new String(RightObjectFilter);
		this.RightPredicateFilter = new String(RightPredicateFilter);
		this.RightConfidenceFilter = RightConfidenceFilter;
		this.LeftOutNodePosition = LeftOutNodePosition;
		this.OutputRightSubject = OutputRightSubject;
		this.OutputRightObject = OutputRightObject;
		get_from_outer = true;
		inner = null;
		outer_tuple = null;
		inner_tuple = null;
		done  = false;

	}

	public  BasicPattern get_next()
		throws IOException,
		       heap.InvalidTypeException, 
		       PageNotReadException,
		       TupleUtilsException, 
		       SortException,
		       LowMemException,
		       UnknowAttrType,
		       UnknownKeyTypeException,
		       Exception
		       {
			       // This is a DUMBEST form of a join, not making use of any key information...
			       if (done)
				       return null;
			       do
			       {
				       // If get_from_outer is true, Get a tuple from the outer, delete
				       // an existing scan on the file, and reopen a new scan on the file.
				       // If a get_next on the outer returns DONE?, then the nested loops
				       //join is done too.

				       if (get_from_outer == true)
				       {
					       get_from_outer = false;
					       if (inner != null)     // If this not the first time,
						       {
							       // close scan
					    	       //inner.closescan(); //XXX
					    	   		inner.closeStream();
							       inner = null;
						       }

					       try {
						       //Triple_HF = SystemDefs.JavabaseDB.getTrpHandle();
						       //inner = Triple_HF.openScan(); //XXX
					    	   if(inner!=null)
					    	   {
					    		   inner.closeStream();
					    	   }
						       inner = SystemDefs.JavabaseDB.openStreamWithoutSort(SystemDefs.JavabaseDB.db_name(), RightSubjectFilter, RightPredicateFilter, RightObjectFilter, RightConfidenceFilter);
					       }
					       catch(Exception e){
						       throw new NestedLoopException(e, "openScan failed");
					       }

					       if ((outer_tuple=left_iter.get_next()) == null)
					       {
						       done = true;
						       if (inner != null) 
						       {
						    	   //inner.closescan(); //XXX
						    	   inner.closeStream();
							       inner = null;
						       }
						       
						       return null;
					       }   
				       }  // ENDS: if (get_from_outer == TRUE)


				       // The next step is to get a tuple from the inner,
				       // while the inner is not completely scanned && there
				       // is no match (with pred),get a tuple from the inner.


				       TID tid = new TID();
				       LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
				       LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
				       while ((inner_tuple = inner.getNextWTSort(tid)) != null)
				       {
					       double confidence = 0;
					       confidence = inner_tuple.getConfidence();
					       Label subject = Entity_HF.getRecord(inner_tuple.getSubjectID().returnLID());
					       Label predicate = Predicate_HF.getRecord(inner_tuple.getPredicateID().returnLID());
					       Label object = Entity_HF.getRecord(inner_tuple.getObjectID().returnLID());
					       boolean result = true;
					       if(RightSubjectFilter.compareToIgnoreCase("null") != 0)
					       {
						       result = result & (RightSubjectFilter.compareTo(subject.getLabelKey()) == 0);	
					       }
					       if(RightObjectFilter.compareToIgnoreCase("null") != 0)
					       {
						       result = result & (RightObjectFilter.compareTo(object.getLabelKey()) == 0 );	
					       }
					       if(RightPredicateFilter.compareToIgnoreCase("null") != 0)
					       {
						       result = result & (RightPredicateFilter.compareTo(predicate.getLabelKey()) == 0 );	
					       }
					       if(RightConfidenceFilter != 0)
					       {
						       result = result & (confidence >= RightConfidenceFilter);
					       }
					       if(result)	
					       {
						       ArrayList<EID> arrEID = new ArrayList<EID>();
						       EID eid_o = outer_tuple.getEIDFld(BPJoinNodePosition);
						       EID eid_i; 
						       if(JoinOnSubjectorObject == 0)
							       eid_i = inner_tuple.getSubjectID();
						       else
							       eid_i = inner_tuple.getObjectID();
						       double min_conf = 0.0;
						       if(confidence <= outer_tuple.getDoubleFld(outer_tuple.noOfFlds()))
							       min_conf = confidence;
						       else
							       min_conf = outer_tuple.getDoubleFld(outer_tuple.noOfFlds());
						       if(eid_o.equals(eid_i))
						       {	
							       BasicPattern bp = new BasicPattern();
							       for(int i = 1; i <= outer_tuple.noOfFlds() - 1;i++)	
							       {
								       for(int j = 0; j < LeftOutNodePosition.length; j++)
								       {
									       if(LeftOutNodePosition[j] == i)
									       {
										       arrEID.add(outer_tuple.getEIDFld(i));
										       break;
									       }
								       }
							       }
							       if(OutputRightSubject == 1 && JoinOnSubjectorObject == 0)//&& !outer_tuple.findEID(inner_tuple.getSubjectID()))
							       {
								       boolean isPresent = false;
								       /*for( int k = 0 ; k < arrEID.size() ; k++)
								       {
									       if(arrEID.get(k).equals(inner_tuple.getSubjectID()) && k == this.BPJoinNodePosition)
									       {
										       isPresent = true;
										       break;
									       }
								       }*/
								       for(int k = 0; k < LeftOutNodePosition.length; k++)
								       {
								    	   if(LeftOutNodePosition[k] == BPJoinNodePosition)
								    	   {
								    		   isPresent = true;
								    		   break;
								    	   }
								       }
								       if(!isPresent)
									       arrEID.add(inner_tuple.getSubjectID());
							       }
							       else if(OutputRightSubject == 1 && JoinOnSubjectorObject == 1)
							       {
								       arrEID.add(inner_tuple.getSubjectID());
							       }
							       if(OutputRightObject == 1 && JoinOnSubjectorObject == 1) //&& !outer_tuple.findEID(inner_tuple.getObjectID()))
							       {
								       boolean isPresent = false;
								       /*for( int k = 0 ; k < arrEID.size() ; k++)
								       {
									       EID eid_out = arrEID.get(k);
									       EID eid_inn = inner_tuple.getObjectID();
									       if(eid_out.equals(eid_inn) && k == this.BPJoinNodePosition)
									       {
										       isPresent = true;
										       break;
									       }
								       }*/
								       for(int k = 0; k < LeftOutNodePosition.length; k++)
								       {
								    	   if(LeftOutNodePosition[k] == BPJoinNodePosition)
								    	   {
								    		   isPresent = true;
								    		   break;
								    	   }
								       }
								       if(!isPresent)
									       arrEID.add(inner_tuple.getObjectID());
							       }
							       else if(OutputRightObject == 1 && JoinOnSubjectorObject == 0)
							       {
								       arrEID.add(inner_tuple.getObjectID());
							       }
							       int k = 0;
							       if(arrEID.size() != 0)
							       {
								       bp.setHdr((short)(arrEID.size()+1));
								       for( k = 0 ; k < arrEID.size() ; k++)
								       {
									       bp.setEIDFld(k+1, arrEID.get(k));
								       }
								       bp.setDoubleFld(k+1,min_conf);
								       
								       /*System.out.println("Outer basic pattern");
									 outer_tuple.print();
									 System.out.println("*********************************");
									 System.out.println("Inner tuples");
									 System.out.println("Subject::" + subject.getLabelKey()+ "\tObject::"+object.getLabelKey()+"\tConfidence::"+confidence );
									 System.out.println("*********************************");
									*/
								       return bp;
							       }

						       }
					       }
				       }

				       // There has been no match. (otherwise, we would have 
				       //returned from t//he while loop. Hence, inner is 
				       //exhausted, => set get_from_outer = TRUE, go to top of loop
				       get_from_outer = true; // Loop back to top and get next outer tuple.	      
			       } while (true);
		       }


	/**
	 * implement the abstract method close() from super class BasicPatternIterator
	 *to finish cleaning up
	 *@exception IOException I/O error from lower layers
	 *@exception JoinsException join error from lower layers
	 *@exception IndexException index access error 
	 */
	public void close() throws IOException, SortException 
	{
		if (!closeFlag) {

			try {
				if(inner!=null)
				{
				//inner.closescan(); XXX
					inner.closeStream();
				}
				left_iter.close();
			}catch (Exception e) {
				System.out.println("NestedLoopsJoin.java: error in closing iterator."+e);
			}
			closeFlag = true;
		}
	}

} 
