/* File BasicPattern.java */

package basicpattern;

import java.io.*;
import java.lang.*;

import labelheap.InvalidLabelSizeException;
import labelheap.InvalidSlotNumberException;
import labelheap.LHFBufMgrException;
import labelheap.LHFDiskMgrException;
import labelheap.LHFException;
import labelheap.Label;
import labelheap.LabelHeapfile;

import global.*;
import heap.*;

public class BasicPattern implements GlobalConst{

	public static SystemDefs sysdef = null;
	
 /** 
  * Maximum size of any basic pattern
  */
  public static final int max_size = MINIBASE_PAGESIZE;

 /** 
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this basic pattern in data[]
   */
  private int basicPattern_offset;

  /**
   * length of this basic pattern
   */
  private int basicPattern_length;

  /** 
   * private field
   * Number of node IDs in this basic pattern
   */
  private short fldCnt;

  /** 
   * private field
   * Array of offsets of the fields
   */
 
  private short [] fldOffset; 

   /**
    * Class constructor
    * Creat a new basic pattern with length = max_size, offset = 0.
    */

  public  BasicPattern()
  {
       // Creat a new tuple
       data = new byte[max_size];
       basicPattern_offset = 0;
       basicPattern_length = max_size;
  }
   
   /** Constructor
    * @param apattern a byte array which contains the pattern
    * @param offset the offset of the pattern in the byte array
    * @param length the length of the pattern
    */

   public BasicPattern(byte [] apattern, int offset, int length)
   {
      data = apattern;
      basicPattern_offset = offset;
      basicPattern_length = length;
    //  fldCnt = getShortValue(offset, data);
   }
   
   /** Constructor(used as basic pattern copy)
    * @param fromPattern   a byte array which contains the pattern
    * 
    */
   public BasicPattern(BasicPattern fromPattern)
   {
       data = fromPattern.getBasicPatternByteArray();
       basicPattern_length = fromPattern.getLength();
       basicPattern_offset = 0;
       fldCnt = fromPattern.noOfFlds(); 
       fldOffset = fromPattern.copyFldOffset(); 
   }

   /**  
    * Class constructor
    * Creat a new basic pattern with length = size,tuple offset = 0.
    */
 
   public void SetBasicPatternArray(byte[] recordarray)
   {
	   data = recordarray;
   }
  public  BasicPattern(int size)
  {
       // Creat a new tuple
       data = new byte[size];
       basicPattern_offset = 0;
       basicPattern_length = size;     
  }

  public BasicPattern(Tuple tuple)
  {
	
	  data = new byte[max_size];
      basicPattern_offset = 0;
      basicPattern_length = max_size;	
	
	  try
	  {
		  int no_tuple_fields = tuple.noOfFlds();
		  setHdr((short)((no_tuple_fields - 1)/2 + 1));
		  int j = 1;	
		  for(int i = 1; i < fldCnt; i++)
		  {
			  int slotno = tuple.getIntFld(j++);
			  int pageno = tuple.getIntFld(j++);
			  PageId page = new PageId(pageno);
			  LID lid = new LID(page,slotno);
			  EID eid = lid.returnEID();
			  setEIDFld(i,eid);	 			
		  }
		  setDoubleFld(fldCnt,(double)tuple.getDoubleFld(j));
	  }
	  catch(Exception e)
	  {
		  System.out.println("Error creating basic pattern from tuple"+e);
	  }
  }	 	
   

   /** Copy a basic pattern to the current basic pattern position
    *  you must make sure the pattern lengths must be equal
    * @param fromPattern the pattern being copied
    */
   public void basicPatternCopy(BasicPattern fromPattern)
   {
       byte [] temparray = fromPattern.getBasicPatternByteArray();
       System.arraycopy(temparray, 0, data, basicPattern_offset, basicPattern_length);   
//       fldCnt = fromTuple.noOfFlds(); 
//       fldOffset = fromTuple.copyFldOffset(); 
   }

   /** This is used when you don't want to use the constructor
    * @param apattern  a byte array which contains the pattern
    * @param offset the offset of the pattern in the byte array
    * @param length the length of the pattern
    */

   public void basicPatternInit(byte [] apattern, int offset, int length)
   {
      data = apattern;
      basicPattern_offset = offset;
      basicPattern_length = length;
   }

 /**
  * Set a basic pattern with the given length and offset
  * @param	record	a byte array contains the pattern
  * @param	offset  the offset of the pattern ( =0 by default)
  * @param	length	the length of the pattern
  */
 public void basicPatternSet(byte [] record, int offset, int length)  
  {
      System.arraycopy(record, offset, data, 0, length);
      basicPattern_offset = 0;
      basicPattern_length = length;
  }
  
 /** get the length of a pattern, call this method if you did not 
  *  call setHdr () before
  * @return 	length of this pattern in bytes
  */   
  public int getLength()
   {
      return basicPattern_length;
   }

/** get the length of a basic pattern, call this method if you did 
  *  call setHdr () before
  * @return     size of this pattern in bytes
  */
  public short size()
   {
      return ((short) (fldOffset[fldCnt] - basicPattern_offset));
   }
 
   /** get the offset of a pattern
    *  @return offset of the pattern in byte array
    */   
   public int getOffset()
   {
      return basicPattern_offset;
   }   
   
   /** Copy the pattern byte array out
    *  @return  byte[], a byte array contains the pattern
    *		the length of byte[] = length of the pattern
    */
    
   public byte [] getBasicPatternByteArray() 
   {
       byte [] patterncopy = new byte [basicPattern_length];
       System.arraycopy(data, basicPattern_offset, patterncopy, 0, basicPattern_length);
       return patterncopy;
   }
   
   public Tuple getTuplefromBasicPattern()
   {
	   Tuple tuple1 = new Tuple();
	   int length = (fldCnt);
	   AttrType[]	 types = new AttrType[(length-1)*2 +1];
	   int j = 0;
	   for(j = 0 ; j < (length-1)*2  ; j++)
	   {
		   types[j] = new AttrType(AttrType.attrInteger);
	   }
	   types[j] = new AttrType(AttrType.attrDouble);
	   short[] s_sizes = new short[1];
	   s_sizes[0] = (short)((length-1)*2 * 4 + 1* 8);
	   try {
		   tuple1.setHdr((short)((length-1)*2 +1) , types, s_sizes);
	   } catch (InvalidTypeException e1) {
		   // TODO Auto-generated catch block
		   e1.printStackTrace();
	   } catch (InvalidTupleSizeException e1) {
		   // TODO Auto-generated catch block
		   e1.printStackTrace();
	   } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
	   int i = 0;
	   j = 1;
	   for( i = 0 ; i < fldCnt-1 ; i++)
	   {
		   try {
			   EID eid = getEIDFld(i+1);
			   tuple1.setIntFld(j++, eid.slotNo);
			   tuple1.setIntFld(j++, eid.pageNo.pid);
		   } catch (FieldNumberOutOfBoundException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
		   } catch (IOException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
		   }
	   }
	   try {
		   tuple1.setDoubleFld(j,getDoubleFld(fldCnt));
	   } catch (FieldNumberOutOfBoundException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
	   /*	  	try {
			tuple1.print(types);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	   return tuple1;
   }
   
   /** return the data byte array 
    *  @return  data byte array 		
    */
    
   public byte [] returnBasicPatternByteArray()
   {
       return data;
   }


   
   /**
    * Convert this field into EID 
    * 
    * @param	fldNo	the field number
    * @return		the converted integer if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

  public EID getEIDFld(int fldNo) 
  	throws IOException, FieldNumberOutOfBoundException
  {           
    int pageno, slotno;
    if ( (fldNo > 0) && (fldNo <= fldCnt))
     {
      pageno = Convert.getIntValue(fldOffset[fldNo -1], data);
      slotno = Convert.getIntValue(fldOffset[fldNo -1] + 4, data);
      PageId page = new PageId();
      page.pid = pageno;		
      LID lid = new LID(page,slotno);	
      EID eid = new EID(lid);		
      return eid;
     }
    else 
     throw new FieldNumberOutOfBoundException (null, "BP:BasicPattern_FLDNO_OUT_OF_BOUND");
  }
  

   /**
    * Convert this field in to double
    *
    * @param    fldNo   the field number
    * @return           the converted double number  if success
    *			
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
    */

    public double getDoubleFld(int fldNo) 
    	throws IOException, FieldNumberOutOfBoundException
     {
	double val;
      if ( (fldNo > 0) && (fldNo <= fldCnt))
       {
        val = Convert.getDoubleValue(fldOffset[fldNo -1], data);
        return val;
       }
      else 
       throw new FieldNumberOutOfBoundException (null, "BP:BasicPattern_FLDNO_OUT_OF_BOUND");
     }


  /**
   * Set this field to EID value
   *
   * @param	fldNo	the field number
   * @param	val	the EID value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public BasicPattern setEIDFld(int fldNo, EID val) 
  	throws IOException, FieldNumberOutOfBoundException
  { 
    if ( (fldNo > 0) && (fldNo <= fldCnt))
     {
	Convert.setIntValue (val.pageNo.pid, fldOffset[fldNo -1], data);
	Convert.setIntValue (val.slotNo, fldOffset[fldNo -1]+4, data);
	return this;
     }
    else 
     throw new FieldNumberOutOfBoundException (null, "BasicPattern :BASIC_PATTERN_FLDNO_OUT_OF_BOUND"); 
  }


  /**
   * Set this field to double value
   *
   * @param     fldNo   the field number
   * @param     val     the double value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
   */

  public BasicPattern setDoubleFld(int fldNo, double val) 
  	throws IOException, FieldNumberOutOfBoundException
  { 
   if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
     Convert.setDoubleValue (val, fldOffset[fldNo -1], data);
     return this;
    }
    else  
     throw new FieldNumberOutOfBoundException (null, "BasicPattern:BASIC PATTERN_FLDNO_OUT_OF_BOUND"); 
     
  }


   /**
    * setHdr will set the header of this tuple.   
    *
    * @param	numFlds	  number of nodeIds + 1 (for confidence)
    *				
    * @exception IOException I/O errors
    * @exception InvalidTypeException Invalid tupe type
    * @exception InvalidTupleSizeException Tuple size too big
    *
    */

  public void setHdr (short numFlds) throws InvalidBasicPatternSizeException, IOException
  {
	  if((numFlds +2)*2 > max_size)
		  throw new InvalidBasicPatternSizeException (null, "BASIC PATTERN: BASIC PATTERN_TOOBIG_ERROR");

	  fldCnt = numFlds;
	  Convert.setShortValue(numFlds, basicPattern_offset, data);
	  fldOffset = new short[numFlds+1];
	  int pos = basicPattern_offset+2;  // start position for fldOffset[]

	  //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
	  //another 1 for fldCnt
	  fldOffset[0] = (short) ((numFlds +2) * 2 + basicPattern_offset);   

	  Convert.setShortValue(fldOffset[0], pos, data);
	  pos +=2;
	  short strCount =0;
	  short incr;
	  int i;

	  for (i=1; i<numFlds; i++)
	  {
		  incr = 8; 	
		  fldOffset[i]  = (short) (fldOffset[i-1] + incr);
		  Convert.setShortValue(fldOffset[i], pos, data);
		  pos +=2;

	  }

	  // For confidence
	  incr = 8; 	

	  fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
	  Convert.setShortValue(fldOffset[numFlds], pos, data);

	  basicPattern_length = fldOffset[numFlds] - basicPattern_offset;

	  if(basicPattern_length > max_size)
		  throw new InvalidBasicPatternSizeException (null, "BASIC PATTERN: BASIC PATTERN_TOOBIG_ERROR");
  }
     
  
  /**
   * Returns number of fields in this tuple
   *
   * @return the number of fields in this tuple
   *
   */

  public short noOfFlds() 
   {
     return fldCnt;
   }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   *
   */

  public short[] copyFldOffset() 
   {
     short[] newFldOffset = new short[fldCnt + 1];
     for (int i=0; i<=fldCnt; i++) {
       newFldOffset[i] = fldOffset[i];
     }
     
     return newFldOffset;
   }

 /**
  * Print out the basic pattern
  * @Exception IOException I/O exception
  */
public void print()
	throws IOException 
{
	int val;
	double dval;
	String sval;
	LabelHeapfile Entity_HF = sysdef.JavabaseDB.getEntityHandle();
	System.out.print("[");
	try {		
		for(int i = 1 ; i <= fldCnt -1 ; i++)
		{
			Label subject = Entity_HF.getRecord(this.getEIDFld(i).returnLID());
			System.out.printf("%30s  ",subject.getLabelKey());
		}
		System.out.print(getDoubleFld(fldCnt));
		System.out.println("]");
		
	} catch (InvalidSlotNumberException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidLabelSizeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LHFException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LHFDiskMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LHFBufMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (FieldNumberOutOfBoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}


public void printIDs()
{
	int val;
	double dval;
	String sval;
	System.out.print("[");
	try {		
		for(int i = 1 ; i <= fldCnt -1 ; i++)
		{
			System.out.print("(" + this.getEIDFld(i).pageNo.pid + "," + this.getEIDFld(i).slotNo + ")");
		}
		System.out.print("Confidence:: "+getDoubleFld(fldCnt));
		System.out.println("]");
		
	} catch (Exception e) {
		System.out.println("Error printing BP"+e);
	}	
}


public boolean findEID(EID eid)
{
	boolean found = false;		
	try
	{
	EID e = null;	
	for (int i=1; i<= fldCnt-1; i++)
	{
		if(eid.equals(getEIDFld(i)))
		{
			found = true;
			break;
		}
			
	}
	}
	catch(Exception e)
	{
		System.out.print(e);

	}		
	return found;
}


}

