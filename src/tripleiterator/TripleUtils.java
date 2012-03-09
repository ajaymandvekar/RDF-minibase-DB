package tripleiterator;

import labelheap.*;
import tripleheap.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing Tuple 
 */
public class TripleUtils
{
  
  private static int compareSubject(Triple t1, Triple t2, LabelHeapfile Elhf)
  { 
	try
	{
		Label S1, S2;
		String t1_s, t2_s;
		LID lid1, lid2;
		char[] c = new char[1];
		c[0] = Character.MIN_VALUE;
		lid1 = t1.getSubjectID().returnLID();
		lid2 = t2.getSubjectID().returnLID();
		if(lid1.pageNo.pid<0)	t1_s = new String(c);
		else {
			S1 = Elhf.getRecord(lid1);		// Comparing Subjects
			t1_s = S1.getLabelKey();
		}
		if(lid2.pageNo.pid<0)	t2_s = new String(c);
		else {
			S2 = Elhf.getRecord(lid2);
			t2_s = S2.getLabelKey();
		}
		if (t1_s.compareTo( t2_s)>0)return 1;
		if (t1_s.compareTo( t2_s)<0)return -1;
		return 0;
	}
	catch(Exception e)
	{	
		System.out.println("Exception" +e);
		return -2;
	}
  }
   
  private static int comparePredicate(Triple t1, Triple t2, LabelHeapfile Plhf)
  { 
	try
	{
	Label P1, P2;
	String t1_s, t2_s;
	LID lid1, lid2;
    	char[] c = new char[1];
    	c[0] = Character.MIN_VALUE;
	lid1 = t1.getPredicateID().returnLID();
	lid2 = t2.getPredicateID().returnLID();
	if(lid1.pageNo.pid<0)	t1_s = new String(c);
	else {
		P1 = Plhf.getRecord(lid1);		// Comparing Predicates
		t1_s = P1.getLabelKey();
	}
	if(lid2.pageNo.pid<0)	t2_s = new String(c);
	else {
		P2 = Plhf.getRecord(lid2);
		t2_s = P2.getLabelKey();
	}
	if (t1_s.compareTo( t2_s)>0)return 1;
	if (t1_s.compareTo( t2_s)<0)return -1;
	return 0;
	}
	catch(Exception e)
	{
		System.out.println("Exception" +e);
		return -2;
	}
  }
    
  private static int compareObject(Triple t1, Triple t2, LabelHeapfile Elhf)
  {
	  try
	  { 
		  Label O1, O2;
		  String t1_s, t2_s;
		  LID lid1, lid2;
		  char[] c = new char[1];
		  c[0] = Character.MIN_VALUE;
		  lid1 = t1.getObjectID().returnLID();
		  lid2 = t2.getObjectID().returnLID();
		  if(lid1.pageNo.pid<0)	t1_s = new String(c);
		  else {
			  O1 = Elhf.getRecord(lid1);		// Comparing Objects
			  t1_s = O1.getLabelKey();
		  }
		  if(lid2.pageNo.pid<0)	t2_s = new String(c);
		  else {
			  O2 = Elhf.getRecord(lid2);
			  t2_s = O2.getLabelKey();
		  }
		  if (t1_s.compareTo( t2_s)>0)return 1;
		  if (t1_s.compareTo( t2_s)<0)return -1;
		  return 0;
	  }
	  catch(Exception e)
	  {
		  System.out.println("Exception" +e);
		  return -2;
	  }
  }
    
  private static int compareConfidence(Triple t1, Triple t2)
  {	
	  try
	  { 
		  double t1_f, t2_f;
		  t1_f = t1.getConfidence();					// Comparing Confidence
		  t2_f = t2.getConfidence();
		  if (t1_f <  t2_f) return -1;
		  if (t1_f >  t2_f) return  1;
		  return  0;
	  }
	  catch(Exception e)
	  {
		  System.out.println("Exception" +e);
		  return -2;
	  }

  }
 
  /**
   * This function compares a triple with another triple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the triple is greater,
   *   -1        if the triple is smaller,
   *
   *@param    t1        one triple.
   *@param    t2        another triple.
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the triple is greater,
   *         -1        if the triple is smaller,                              
   */
  public static int CompareTripleWithTriple(TripleOrder orderType, Triple t1, Triple t2)
  throws TripleUtilsException
  {
	  LabelHeapfile Elhf = SystemDefs.JavabaseDB.getEntityHandle();
	  LabelHeapfile Plhf = SystemDefs.JavabaseDB.getPredicateHandle();
	  int retVal = -2;

	  switch (orderType.tripleOrder) 
	  {
		  case TripleOrder.SubjectPredicateObjectConfidence:              // S, P, O, C
			  retVal = compareSubject(t1,t2,Elhf);
			  if (retVal==0) 
			  {
				  retVal = comparePredicate(t1,t2,Plhf);
				  if(retVal==0) {
					  retVal = compareObject(t1,t2,Elhf);
					  if(retVal==0) {
						  retVal = compareConfidence(t1,t2);
					  }
				  }
			  }
			  return retVal;
		  case TripleOrder.PredicateSubjectObjectConfidence:              // P, S, O, C
			  retVal = comparePredicate(t1,t2,Plhf);
			  if (retVal==0) 
			  {
				  retVal = compareSubject(t1,t2,Elhf);
				  if(retVal==0) {
					  retVal = compareObject(t1,t2,Elhf);
					  if(retVal==0) {
						  retVal = compareConfidence(t1,t2);
					  }
				  }
			  }
			  return retVal;
		  case TripleOrder.SubjectConfidence:                		// S, C
			  retVal = compareSubject(t1,t2,Elhf);
			  if (retVal==0) {
				  retVal = compareConfidence(t1,t2);
			  }
			  return retVal;
		  case TripleOrder.PredicateConfidence:                		// P, C
			  retVal = comparePredicate(t1,t2,Plhf);
			  if (retVal==0) {
				  retVal = compareConfidence(t1,t2);
			  }
			  return retVal;
		  case TripleOrder.ObjectConfidence:                		// O, C
			  retVal = compareObject(t1,t2,Elhf);
			  if (retVal==0) {
				  retVal = compareConfidence(t1,t2);
			  }
			  return retVal;
		  case TripleOrder.Confidence:                			// C
			  retVal = compareConfidence(t1,t2);
			  return retVal;
		  default: 
			  return retVal;
	  }
  }
  
  /**
   *set up a triple in specified field from a triple
   *@param value the triple to be set 
   *@param triple the given triple
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */  
  public static void SetValue(Triple value, Triple  triple)
  {
	  value.tripleCopy(triple); 
  }
}




