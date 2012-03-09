package basicpatterniterator;


import heap.*;
import global.*;

import java.io.*;
import labelheap.InvalidLabelSizeException;
import labelheap.InvalidSlotNumberException;
import labelheap.LHFBufMgrException;
import labelheap.LHFDiskMgrException;
import labelheap.LHFException;
import labelheap.Label;
import labelheap.LabelHeapfile;

/**
 *some useful method when processing Tuple 
 */
public class BasicPatternUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple.
   *@param    t2        another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.
   *@param    t2_fld_no the field numbers in the tuples to be compared. 
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
 * @throws Exception 
 * @throws LHFBufMgrException 
 * @throws LHFDiskMgrException 
 * @throws LHFException 
 * @throws InvalidLabelSizeException 
 * @throws InvalidSlotNumberException 
   */
  public static int CompareTupleWithTuple(AttrType fldType,
					  Tuple  t1, int t1_fld_no,
					  Tuple  t2, int t2_fld_no)
    throws InvalidSlotNumberException, InvalidLabelSizeException, LHFException, LHFDiskMgrException, LHFBufMgrException, Exception
    {
	int t1_epid, t1_esid,
	    t2_epid, t2_esid;
      	double t1_r,  t2_r;
	String t1_s, t2_s;
    char[] c_min = new char[1];
    c_min[0] = Character.MIN_VALUE; 
    String s_min = new String(c_min);
    char[] c_max = new char[1];
    c_max[0] = Character.MAX_VALUE; 
    String s_max = new String(c_max);
    LabelHeapfile Elhf = SystemDefs.JavabaseDB.getEntityHandle();

      switch (fldType.attrType) 
	{
	case AttrType.attrInteger:                // Compare two integers.
	  try {
		t1_esid = t1.getIntFld(t1_fld_no);
		t1_epid = t1.getIntFld(t1_fld_no+1);
		t2_esid = t2.getIntFld(t2_fld_no);
		t2_epid = t2.getIntFld(t2_fld_no+1);
		PageId t1_pid = new PageId(t1_epid);
		PageId t2_pid = new PageId(t2_epid);
		LID t1_lid = new LID(t1_pid,t1_esid);
		LID t2_lid = new LID(t2_pid,t2_esid);
		Label S1, S2;
                if(t1_lid.pageNo.pid<0)   			t1_s = new String(s_min);
		else if(t1_lid.pageNo.pid==Integer.MAX_VALUE)   t1_s = new String(s_max);
                else {
                        S1 = Elhf.getRecord(t1_lid);              // Comparing Entities
                        t1_s = S1.getLabelKey();
                }
                if(t2_lid.pageNo.pid<0)   			t2_s = new String(s_min);
		else if(t2_lid.pageNo.pid==Integer.MAX_VALUE)  	t2_s = new String(s_max);
                else {
                        S2 = Elhf.getRecord(t2_lid);
                        t2_s = S2.getLabelKey();
                } 
	  	}catch (FieldNumberOutOfBoundException e){
	    		throw new BasicPatternUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  	}
                if (t1_s.compareTo( t2_s)>0)return 1;
                if (t1_s.compareTo( t2_s)<0)return -1;
                return 0;

	case AttrType.attrDouble:                // Compare two floats
	  try {
	    t1_r = t1.getDoubleFld(t1_fld_no);
	    t2_r = t2.getDoubleFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BasicPatternUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  if (t1_r == t2_r) return  0;
	  if (t1_r <  t2_r) return -1;
	  if (t1_r >  t2_r) return  1;
	  
	default:
	  
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
    }
  
  
  
  /**
   * This function  compares  tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple
   *@param    value     another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.  
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,  
 * @throws Exception 
 * @throws LHFBufMgrException 
 * @throws LHFDiskMgrException 
 * @throws LHFException 
 * @throws InvalidLabelSizeException 
 * @throws InvalidSlotNumberException 
   *@exception TupleUtilsException exception from this class   
   */            
  public static int CompareTupleWithValue(AttrType fldType,
					  Tuple  t1, int t1_fld_no,
					  Tuple  value)
    throws InvalidSlotNumberException, InvalidLabelSizeException, LHFException, LHFDiskMgrException, LHFBufMgrException, Exception
    {
      return CompareTupleWithTuple(fldType, t1, t1_fld_no, value, t1_fld_no);
    }
  
 
  /**
   *set up a tuple in specified field from a tuple
   *@param value the tuple to be set 
   *@param tuple the given tuple
   *@param fld_no the field number
   *@param fldType the tuple attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */  
  public static void SetValue(Tuple value, Tuple  tuple, int fld_no, AttrType fldType)
    throws IOException,
	   UnknowAttrType,
	   BasicPatternUtilsException
    {
      
      switch (fldType.attrType)
	{
	case AttrType.attrInteger:
	  try {
	    value.setIntFld(fld_no, tuple.getIntFld(fld_no));
	    value.setIntFld(fld_no+1, tuple.getIntFld(fld_no+1));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BasicPatternUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	case AttrType.attrDouble:
	  try {
	    value.setDoubleFld(fld_no, tuple.getDoubleFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BasicPatternUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	default:
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
      
      return;
    }
}
  

