package tests;

import diskmgr.*;
import global.*;
import java.io.*;
import tripleheap.*;
import java.lang.*;
import labelheap.*;
import tripleiterator.*;

public class RDFSortTest
{
	public static void main(String[] args)
	{
		String dbname = null;   //Database name 
		int indexoption = 0;    //Index option
		boolean exists = false;

		if(args.length == 2 )   //Check if the args are DATAFILE DATABASENAME INDEXOPTION
		{
			indexoption = Integer.parseInt(args[1]);
			dbname = new String("/tmp/"+args[0]+"_"+indexoption);

			if(indexoption>5 || indexoption<0)
			{
				System.out.println("*** Indexoption only allowed within range: 1 to 5 ***");
				return;
			}
		}
		else
		{
			System.out.println("*** Usage:RDFSortTest RDFDBNAME INDEXOPTION***");
			return;
		}


		EID sid = null, oid = null;
		PID pid = null;
		Triple t = null;
		TID tid = null;
		SystemDefs sysdef = null;
		int counter = 0;

		File dbfile = new File(dbname); //Check if database already exsist
		if(dbfile.exists())
		{
			//Database already present just open it
			sysdef = new SystemDefs(dbname,0,500,"Clock",indexoption);
			System.out.println("*** Opening existing database ***");
		}
		else
		{	
			System.out.println("*** NO DB ***");
			return;
		}

		try
		{
			TScan am = new TScan(sysdef.JavabaseDB.getTrpHandle());
			TripleOrder sort_order = new TripleOrder(TripleOrder.PredicateConfidence);
			TripleSort tsort = new TripleSort(am, sort_order , 200);
			int count = 0;
			Triple triple = null;
			while((triple = tsort.get_next()) != null)
			{
				System.out.println("Confidence--> "+triple.getConfidence());
				//System.out.println(triple.getSubjectID());
				LabelHeapfile l1 = sysdef.JavabaseDB.getEntityHandle();
				Label subject = l1.getRecord(triple.getSubjectID().returnLID());
				System.out.println("Subject--> "+subject.getLabelKey());
				LabelHeapfile l2 = sysdef.JavabaseDB.getPredicateHandle();
				Label predicate = l2.getRecord(triple.getPredicateID().returnLID());
				System.out.println("Predicate--> "+predicate.getLabelKey());
				LabelHeapfile l3 = sysdef.JavabaseDB.getEntityHandle();
				Label object = l3.getRecord(triple.getObjectID().returnLID());
				System.out.println("Object--> "+object.getLabelKey());
				System.out.println("*****************************");
				count++;
			}
			tsort.close();
			System.out.println("-- Count="+count +" --");
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		System.out.println("** SORTING DONE **");
		return ;
	}
}
