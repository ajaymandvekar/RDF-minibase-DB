package tests;

import java.io.*; 

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import tripleheap.*;

class TRIPLEDriver extends TestDriver implements GlobalConst {

	public TRIPLEDriver() {
		super("tripletest");
	}

	public boolean runTests ()  {

		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} 
		catch (IOException e) {
			System.err.println (""+e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		//This step seems redundant for me.  But it's in the original
		//C++ code.  So I am keeping it as of now, just in case I
		//I missed something
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} 
		catch (IOException e) {
			System.err.println (""+e);
		}

		//Run the tests. Return type different from C++
		boolean _pass = runAllTests();

		//Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} 
		catch (IOException e) {
			System.err.println (""+e);
		}

		System.out.println ("\n" + "..." + testName() + " tests ");
		System.out.println (_pass==OK ? "completely successfully" : "failed");
		System.out.println (".\n\n");

		return _pass;
	}

	protected boolean test1()
	{
		System.out.println("------------------------ TEST 1 --------------------------");

		boolean status = OK;

		// create a triple of appropriate size
		Triple t = new Triple();
		int size = t.size();

		// Create unsorted data file "test1.in"
		TID             tid;
		TripleHeapfile        f = null;
		try 
		{
			f = new TripleHeapfile("test1.in");
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		t = new Triple();
		try {
			t.setConfidence(1.5);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			tid = f.insertTriple(t.returnTripleByteArray());
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		System.err.println("------------------- TEST 1 completed ---------------------\n");

		return status;
	}

	protected String testName()
	{
		return "Triple";
	}
}

public class TripleTest
{
	public static void main(String argv[])
	{
		boolean triplestatus;

		TRIPLEDriver tripleDriver = new TRIPLEDriver();

		triplestatus = tripleDriver.runTests();

		if (triplestatus != true) {
			System.out.println("Error ocurred during triple tests");
		}
		else {
			System.out.println("Triple tests completed successfully");
		}
	}
}
