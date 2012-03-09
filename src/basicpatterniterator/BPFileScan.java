package basicpatterniterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import basicpattern.*;
import iterator.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class BPFileScan extends  BasicPatternIterator
{
  private Heapfile f;
  private Scan scan;
  private Tuple     tuple1;
  public AttrType[] types;
  public short[] s_sizes;
  public int length;

  public  BPFileScan (String  file_name,int no_fields)
	  throws IOException,
		 FileScanException,
		 TupleUtilsException, 
		 InvalidRelation
		 {
			 tuple1 =  new Tuple();

			 try {
				 f = new Heapfile(file_name);
			 }
			 catch(Exception e) {
				 throw new FileScanException(e, "Create new heapfile failed");
			 }
			 //Each field in the tuple is stored alongwith the offset in the data array
			 //Each offset will be of 2 bytes..So if there are 3 fields 6 bytes in the data array will store the offset of the fields
			 //2 bytes store field Cnt
			 //2 bytes not known
			 length = (no_fields) ;
			 types = new AttrType[(length-1)*2 +1];
			 int j = 0;
			 for(j = 0 ; j < (length-1)*2  ; j++)
			 {
				 types[j] = new AttrType(AttrType.attrInteger);
			 }
			 types[j] = new AttrType(AttrType.attrDouble);
			 s_sizes = new short[1];
			 s_sizes[0] = (short)((length-1)*2 * 4 + 1* 8);
			 try {
				 tuple1.setHdr((short)((length-1)*2 +1) , types, s_sizes);
			 } catch (InvalidTypeException e1) {
				 // TODO Auto-generated catch block
				 e1.printStackTrace();
			 } catch (InvalidTupleSizeException e1) {
				 // TODO Auto-generated catch block
				 e1.printStackTrace();
			 }

			 try {
				 scan = f.openScan();
			 }
			 catch(Exception e){
				 throw new FileScanException(e, "openScan() failed");
			 }
		 }

  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public BasicPattern get_next()
    throws JoinsException,
	   IOException,
	   InvalidTupleSizeException,
	   InvalidTypeException,
	   PageNotReadException, 
	   PredEvalException,
	   UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   WrongPermat
	   {     
		   RID rid = new RID();

		   while(true) {
			   if((tuple1 =  scan.getNext(rid)) == null) {
				   return null;
			   }
			   //System.out.println("Priting before sethdr");
			   //tuple1.print(types);

			   tuple1.setHdr((short)((length-1)*2+1), types, s_sizes);
			   //System.out.println("\nPrinting after sethr******");
			   //tuple1.print(types);
			   //Each field in the tuple is stored alongwith the offset in the data array
			   //Each offset will be of 2 bytes..So if there are 3 fields 6 bytes in the data array will store the offset of the fields
			   //2 bytes store field Cnt
			   //2 bytes not known
			   short length1 = (tuple1.noOfFlds());

			   BasicPattern bp = new BasicPattern();
			   try {
				   bp.setHdr((short)((length1)/2 + 1));
			   } catch (InvalidBasicPatternSizeException e) {
				   // TODO Auto-generated catch block
				   e.printStackTrace();
			   }
			   int i = 0;
			   int j = 0;
			   for(i = 0 , j = 1; i < (length1/2)  ; i++)
			   {
				   int slotno = tuple1.getIntFld(j++);
				   int pageno = tuple1.getIntFld(j++);

				   LID lid = new LID(new PageId(pageno),slotno);
				   EID eid = lid.returnEID();
				   bp.setEIDFld(i+1, eid);

			   }
			   double minconf = tuple1.getDoubleFld(j);
			   bp.setDoubleFld(i+1, minconf);
			   return bp;
		   }
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}


