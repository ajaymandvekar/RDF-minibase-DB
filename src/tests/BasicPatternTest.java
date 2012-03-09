package tests;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.PredEvalException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

import java.io.*; 

import tripleheap.FieldNumberOutOfBoundException;

import global.*;
import basicpatterniterator.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import basicpattern.*;
import bufmgr.PageNotReadException;

class BasicPatternDriver extends TestDriver implements GlobalConst {

	public BasicPatternDriver() {
		super("basicpatterntest");
	}

	public boolean runTests ()  {
		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
		   
		boolean _pass = runAllTests();
		System.out.println ("\n" + "..." + testName() + " tests ");
		System.out.println (_pass==OK ? "completely successfully" : "failed");
		System.out.println (".\n\n");

		return _pass;
	}

	protected boolean test1()
	{
		System.out.println("------------------------ TEST 1 --------------------------");
		boolean status = OK;
		
		try
		{
			BasicPattern bp = new BasicPattern();
			int num = 3;
			bp.setHdr((short)num);
			PageId pageno = new PageId();
		
			LID lid1 = new LID(pageno,0);
			EID eid1 = new EID(lid1);

			LID lid2 = new LID(pageno,1);
			EID eid2 = new EID(lid2);

			double conf = 0.1437143171111111111111;    //3
				
			bp.setEIDFld(1,eid1);
			bp.setEIDFld(2,eid2);
			bp.setDoubleFld(3,conf);

			bp.printIDs();
				 
			
			System.err.println("\n------------------- TEST 1 completed ---------------------\n");
		}
		catch(Exception e)
		{
			System.out.println("BP Exception" +e);
		}

		return status;

	}


	protected boolean test2()
	{
		System.out.println("------------------------ TEST 2 --------------------------");
		boolean status = OK;
		
		try
		{
			BasicPattern bp = new BasicPattern();
			int num = 7;
			bp.setHdr((short)num);
			PageId pageno = new PageId();
		
			LID lid[] = new LID[num -1];
			EID eid[] = new EID[num -1];
			double conf = 0.143714317656546546465;    //3
				
			for(int i = 0; i < num - 1; i++)
			{
				lid[i] = new LID(pageno,i); //1
				eid[i] = new EID(lid[i]);	
				bp.setEIDFld(i+1,eid[i]);

			}
			
			bp.setDoubleFld(num,conf);
			bp.printIDs();
				 
			
			System.err.println("\n------------------- TEST 2 completed ---------------------\n");
		}
		catch(Exception e)
		{
			System.out.println("BP Exception" +e);
		}

		return status;

	}

	protected boolean test3()
	{
		System.out.println("------------------------ TEST 3 --------------------------");
		boolean status = OK;
		
		try
		{
			BasicPattern bp = new BasicPattern();
			int num = 3;
			bp.setHdr((short)num);
			PageId pageno = new PageId();
		
			LID lid1 = new LID(pageno,0);
			EID eid1 = new EID(lid1);

			LID lid2 = new LID(pageno,1);
			EID eid2 = new EID(lid2);

			double conf = 0.143714317;    //3
				
			bp.setEIDFld(1,eid1);
			bp.setEIDFld(2,eid2);
			bp.setDoubleFld(3,conf);

			bp.printIDs();
				
			EID eid3 = bp.getEIDFld(1);
			EID eid4 = bp.getEIDFld(2);
			double conf1 = bp.getDoubleFld(3);

			System.out.print("("+eid3.pageNo.pid+","+eid3.slotNo+")"); 
			System.out.print("("+eid4.pageNo.pid+","+eid4.slotNo+")"); 
			System.out.print(conf1); 
			
			System.err.println("\n------------------- TEST 3 completed ---------------------\n");
		}
		catch(Exception e)
		{
			System.out.println("BP Exception" +e);
		}

		return status;

	}

	protected boolean test4()
	{
		System.out.println("------------------------ TEST 4 --------------------------");
		boolean status = OK;
		RID rid = new RID();
		Heapfile f = null;

		System.out.println ("  - Create a heap file\n");
		try {
			f = new Heapfile("file123");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}

		if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
				!= SystemDefs.JavabaseBM.getNumBuffers() ) {
			System.err.println ("*** The heap file has left pages pinned\n");
			status = FAIL;
		}

		if ( status == OK ) {
			System.out.println ("  - Add " + "10" + " records to the file\n");
		}
		BasicPattern bp = new BasicPattern();
		int num = 3;
		PageId pageno = new PageId();

		LID lid1 = new LID(pageno,0);
		EID eid1 = new EID(lid1);

		LID lid2 = new LID(pageno,1);
		EID eid2 = new EID(lid2);

		double conf = 0.143714317555557773;    //3

		try {
			bp.setHdr((short)num);
			bp.setEIDFld(1,eid1);
			bp.setEIDFld(2,eid2);
			bp.setDoubleFld(3,conf);

			bp.printIDs();
			EID eid3 = bp.getEIDFld(1);
			EID eid4 = bp.getEIDFld(2);
			double conf1 = bp.getDoubleFld(3);

			/*System.out.print("("+eid3.pageNo.pid+","+eid3.slotNo+")"); 
			System.out.print("("+eid4.pageNo.pid+","+eid4.slotNo+")"); 
			System.out.print(conf1); */

		} catch (InvalidBasicPatternSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (heap.FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			rid = f.insertRecord((bp.getTuplefromBasicPattern()).getTupleByteArray());
			bp.printIDs();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SpaceNotAvailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BasicPattern basicpattern = null;
		try {
			BPFileScan newscan = new BPFileScan("file123",num);
			basicpattern = newscan.get_next();
			//basicpattern = new BasicPattern(t);

		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			//basicpattern.setHdr((short)num);
			//basicpattern.setEIDFld(1,eid1);
			//basicpattern.setEIDFld(2,eid2);
			//basicpattern.setDoubleFld(3,conf);

			basicpattern.printIDs();
			EID eid3 = basicpattern.getEIDFld(1);
			EID eid4 = basicpattern.getEIDFld(2);
			double conf1 = basicpattern.getDoubleFld(3);

			System.out.print("("+eid3.pageNo.pid+","+eid3.slotNo+")"); 
			System.out.print("("+eid4.pageNo.pid+","+eid4.slotNo+")"); 
			System.out.print(conf1); 

		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.err.println("\n------------------- TEST 4 completed ---------------------\n");

		return status;
	}

	protected String testName()
	{
		return "BasicPattern";
	}
	
	/* (non-Javadoc)
	 * @see tests.TestDriver#test5()
	 */
	protected boolean test5()
	{
		boolean status = OK;
		System.out.println("------------------------ TEST 5 --------------------------");
		try
		{
			PageId pageno = new PageId();
			BasicPattern bp = new BasicPattern();
			int num = 3;
			bp.setHdr((short)num);
			LID lid1 = new LID(pageno,0); //1 field
			EID eid1 = new EID(lid1);
			LID lid2 = new LID(pageno,1); //2 field
			EID eid2 = new EID(lid2);
			double conf = 0.14371431712121212;    //3 field
				
			bp.setEIDFld(1,eid1);
			bp.setEIDFld(2,eid2);
			bp.setDoubleFld(3,conf);

			System.out.println("Printing Basic Pattern 1-- ");
			bp.printIDs();
			
			System.out.println("Printing Basic Pattern 2 -- ");
			Tuple t = bp.getTuplefromBasicPattern();
			BasicPattern bp1 = new BasicPattern(t);
			bp1.printIDs();
	
				 
			System.err.println("\n------------------- TEST 5 completed ---------------------\n");
			
		}
		catch(Exception e)
		{
			System.out.println("BP Exception" +e);
		}

		return status;
	}

}



public class BasicPatternTest
{
	public static void main(String argv[])
	{
		boolean bpstatus;

		BasicPatternDriver bpDriver = new BasicPatternDriver();

		bpstatus = bpDriver.runTests();

		if (bpstatus != true) {
			System.out.println("Error ocurred during basic pattern tests");
		}
		else {
			System.out.println("Basic Pattern tests completed successfully");
		}
	}
}
