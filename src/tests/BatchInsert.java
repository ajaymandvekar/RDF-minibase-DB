package tests;

import diskmgr.*;
import global.*;

import java.io.*;
import tripleheap.*;
import tripleiterator.TripleSort;

import java.lang.*;
import java.util.ArrayList;

import btree.KeyClass;
import btree.KeyDataEntry;
import btree.StringKey;
import btree.TripleBTFileScan;
import btree.TripleBTreeFile;
import btree.TripleLeafData;

import labelheap.*;
import labelheap.InvalidSlotNumberException;

public class BatchInsert{

	public static ArrayList<String> entities = new ArrayList<String>();
	public static ArrayList<String> predicates = new ArrayList<String>();
	public static ArrayList<byte[]> triples = new ArrayList<byte[]>();
	public static SystemDefs sysdef = null;
	public static boolean existingdb = false;

	private static void print_triple(Triple triple) 
	throws InvalidSlotNumberException, InvalidLabelSizeException, LHFException, LHFDiskMgrException, LHFBufMgrException, Exception 
	{
		//System.out.println(triple.getSubjectID());
		LabelHeapfile l1 = sysdef.JavabaseDB.getEntityHandle();
		Label subject = l1.getRecord(triple.getSubjectID().returnLID());
		LabelHeapfile l2 = sysdef.JavabaseDB.getPredicateHandle();
		Label predicate = l2.getRecord(triple.getPredicateID().returnLID());
		LabelHeapfile l3 = sysdef.JavabaseDB.getEntityHandle();
		Label object = l3.getRecord(triple.getObjectID().returnLID());
		System.out.println(subject.getLabelKey() + ":" + predicate.getLabelKey() + ":" + object.getLabelKey() + "("+ triple.getConfidence()+")");
	}


	public static void db_stats()
	{
		int reccnt = sysdef.JavabaseDB.getPredicateCnt();
		int triplecnt = sysdef.JavabaseDB.getTripleCnt();
		int subjectcnt = sysdef.JavabaseDB.getSubjectCnt();
		int objectcnt = sysdef.JavabaseDB.getObjectCnt();
		int entitycnt = sysdef.JavabaseDB.getEntityCnt();

		System.out.println("Total Predicate Cnt "+ reccnt );
		System.out.println("Total Triple Count "+ triplecnt);
		System.out.println("Total Subject Count "+ subjectcnt);                   
		System.out.println("Total Object Count "+ objectcnt);                     
		System.out.println("Total Entity Count "+ entitycnt);
	}

	public static void delete_test()
	{

		int i = 0;
		boolean success = false;
		int reccnt = 0,triplecnt = 0,subjectcnt = 0,objectcnt = 0,entitycnt = 0;
		//NOTE: Enable the code below to test for entity deletion

		/*System.out.println("Deleting first 10 entities ");

		  for(i = 0; i < 10; i++)
		  {
		  success = sysdef.JavabaseDB.deleteEntity(entities.get(i));
		  System.out.println("Result of deleting entity " + entities.get(i) + " : " + success);

		  }
		  System.out.println("After deletion");

		  reccnt = sysdef.JavabaseDB.getPredicateCnt();
		  triplecnt = sysdef.JavabaseDB.getTripleCnt();
		  subjectcnt = sysdef.JavabaseDB.getSubjectCnt();
		  objectcnt = sysdef.JavabaseDB.getObjectCnt();
		  entitycnt = sysdef.JavabaseDB.getEntityCnt();

		  System.out.println("Total Predicate Cnt "+ reccnt + "\n");
		  System.out.println("Total Triple Count "+ triplecnt +"\n");                     
		  System.out.println("Total Subject Count "+ subjectcnt +"\n");                   
		  System.out.println("Total Object Count "+ objectcnt +"\n");                     
		  System.out.println("Total Entity Count "+ entitycnt +"\n");*/

		//NOTE: Enable the code below to test for predicate deletion

		/*System.out.println("Deleting first 10 predicates ");

		  for(i = 0; i < 10; i++)
		  {
		  success = sysdef.JavabaseDB.deletePredicate(predicates.get(i));
		  System.out.println("Result of deleting predicate " + predicates.get(i) + " : " + success);

		  }
		  System.out.println("After deletion");

		  reccnt = sysdef.JavabaseDB.getPredicateCnt();
		  triplecnt = sysdef.JavabaseDB.getTripleCnt();
		  subjectcnt = sysdef.JavabaseDB.getSubjectCnt();
		  objectcnt = sysdef.JavabaseDB.getObjectCnt();
		  entitycnt = sysdef.JavabaseDB.getEntityCnt();

		  System.out.println("Total Predicate Cnt "+ reccnt + "\n");
		  System.out.println("Total Triple Count "+ triplecnt +"\n");                     
		  System.out.println("Total Subject Count "+ subjectcnt +"\n");                   
		  System.out.println("Total Object Count "+ objectcnt +"\n");                     
		  System.out.println("Total Entity Count "+ entitycnt +"\n");*/

		//NOTE: Enable the code below to test for triple deletion

		System.out.println("Deleting first 10 triples ");

		/*for(i = 0; i < 10; i++)
		  {
		  success = sysdef.JavabaseDB.deleteTriple(triples.get(i));
		  System.out.println("Result of deleting triple " + triples.get(i) + " : " + success);

		  }
		  System.out.println("After deletion");

		  reccnt = sysdef.JavabaseDB.getPredicateCnt();
		  triplecnt = sysdef.JavabaseDB.getTripleCnt();
		  subjectcnt = sysdef.JavabaseDB.getSubjectCnt();
		  objectcnt = sysdef.JavabaseDB.getObjectCnt();
		  entitycnt = sysdef.JavabaseDB.getEntityCnt();

		  System.out.println("Total Predicate Cnt "+ reccnt + "\n");
		  System.out.println("Total Triple Count "+ triplecnt +"\n");                     
		  System.out.println("Total Subject Count "+ subjectcnt +"\n");                   
		  System.out.println("Total Object Count "+ objectcnt +"\n");                     
		  System.out.println("Total Entity Count "+ entitycnt +"\n");*/

	}

	void index_tests()
	{
		//delete_test();
		//sysdef.JavabaseDB.createIndex1(); 
		//sysdef.JavabaseDB.createIndex2(); 
		//sysdef.JavabaseDB.createIndex3(); 
		//sysdef.JavabaseDB.createIndex4(); 
		//sysdef.JavabaseDB.createIndex5(); 
	}

	public static TID insertTriple(byte[] triplePtr)
	throws Exception
	{
		TID tid = null;
		try
		{
			//Open Temp heap file
			TripleHeapfile Triple_HF = sysdef.JavabaseDB.getTEMP_Triple_HF();
			tid= Triple_HF.insertTriple(triplePtr);
			//System.out.println("Inserting tid : " + tid);
		}
		catch(Exception e)
		{
			System.err.println ("*** Error inserting triple record " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return tid;
	}

	public static void main(String[] args)
	{
		String dbname = null;   //Database name 
		int indexoption = 0;    //Index option
		String datafile = null; //Datafile from which to load the data

		boolean exists = false;

		if(args.length == 3 )   //Check if the args are DATAFILE DATABASENAME INDEXOPTION
		{
			datafile = new String(args[0]); 
			indexoption = Integer.parseInt(args[1]);
			dbname = new String("/tmp/"+args[2]+"_"+indexoption);

			//Check if datafile present
			File file = new File(datafile);
			exists = file.exists();
			if(!exists)
			{
				System.out.println("*** File path:"+datafile+" dosent exist. ***");
				return;
			}

			if(indexoption>5 || indexoption<0)
			{
				System.out.println("*** Indexoption only allowed within range: 1 to 5 ***");
				return;
			}
		}
		else
		{
			System.out.println("*** Usage:BatchInsert DATAFILE INDEXOPTION RDFDBNAME ***");
			return;
		}


		EID sid = null, oid = null;
		PID pid = null;
		Triple t = null;
		TID tid = null;


		File dbfile = new File(dbname); //Check if database already exsist
		if(dbfile.exists())
		{
			//Database already present just open it
			sysdef = new SystemDefs(dbname,0,1000,"Clock",indexoption);
			//System.out.println("*** Opening existing database ***");
			existingdb = true;
		}
		else
		{	
			//Create new database
			sysdef = new SystemDefs(dbname,10000,1000,"Clock",indexoption);
			//System.out.println("*** Creating existing database ***");
		}

		try
		{
			FileInputStream fstream = new FileInputStream(datafile);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			int i = 0;
			double confidence = 0.0;
			//Read File Line By Line
			while ((strLine = br.readLine()) != null)   
			{
				// Print the content on the console
				// System.out.println (strLine);
				//System.out.println(strLine.split("\\s:"));
				i = 0;
				int count = 0;
				int idx = 0;

				while ((idx = strLine.indexOf(":", idx)) != -1)
				{
					idx++;
					count++; 
				}
				
				if(count == 3)
				{
					String[] str = strLine.split("[\\s\\t:]");

					for(String tag: str)
					{
						if(tag.trim().length() > 0)
						{
							if(i == 1)
							{
								try
								{
									sid = sysdef.JavabaseDB.insertEntity(tag);
									//System.out.println(">>"+tag);
									if(i < 10)
										entities.add(tag);
								}
								catch(Exception e)
								{
									System.out.println("Unable to insert SID:+"+tag);
								}
							}
							else if(i == 3)
							{ 
								try
								{
									pid = sysdef.JavabaseDB.insertPredicate(tag);
									//System.out.println(">>"+tag);
									if(i < 10)
										predicates.add(tag);
								}
								catch(Exception e)
								{	
									System.out.println("Unable to insert PID:+"+tag);
								}

							}
							else if(i == 5)
							{
								try
								{
									oid = sysdef.JavabaseDB.insertEntity(tag);
									//System.out.println(">>"+tag);
									if(i < 10)
										entities.add(tag);
								}
								catch(Exception e)
								{
									System.out.println("Unable to insert OID:+"+tag);
								}
							}
							else if(i == 7)
							{
								confidence = Double.parseDouble(tag);
								//System.out.println(">>"+confidence);
							}
						}

						i++;
					}

					//System.out.println("Trying to insert triple RDF");
					t = new Triple();
					t.setSubjectID(sid);
					t.setPredicateID(pid);
					t.setObjectID(oid);
					t.setConfidence(confidence);

					try 
					{
						tid = insertTriple(t.getTripleByteArray());
					}
					catch (Exception e) {
						e.printStackTrace();
					}
				}
				else
				{
					System.out.println("Invalid Record::"+ strLine);
				}
			}
			in.close();

			if(existingdb == true)
			{
				try
				{
					TScan am = new TScan(sysdef.JavabaseDB.getTrpHandle());
					TID t1 = new TID();
					Triple t2 = null;
					while((t2 = am.getNext(t1))!= null)
					{
						System.out.print("##############Scanning earlier records and deleting########");
						print_triple(t2);
						insertTriple(t2.getTripleByteArray());
						sysdef.JavabaseDB.deleteTriple(t2.getTripleByteArray());
					}
				}
				catch(Exception e)
				{
					sort_temporary_heap_file(dbname,indexoption);
				}
			}
			else
			{
				sort_temporary_heap_file(dbname,indexoption);

			}
		}
		catch(Exception e)
		{
			System.out.println("BATCHINSERT ERROR :: " + e);
		}
		System.out.println("-------------------------------------");
		System.out.println(" INDEX OPTIONS: ");
		System.out.println(" 1. BTree Index file on confidence: ");
		System.out.println(" 2. BTree Index file on subject and confidence: ");
		System.out.println(" 3. BTree Index file on object and confidence: ");
		System.out.println(" 4. BTree Index file on predicate and confidence: ");
		System.out.println(" 5. BTree Index file on subject: ");
		System.out.println("-------------------------------------");
		System.out.println(" ||  CREATING INDEX WITH OPTION :(" + indexoption + ")  ||");
		System.out.println("-------------------------------------");
		sysdef.JavabaseDB.createIndex(indexoption);
		
		db_stats();
		sysdef.close();
		
		System.out.println("Total Page Writes "+ PCounter.wcounter);
		System.out.println("Total Page Reads "+ PCounter.rcounter);
		
		System.out.println(" $$$$$$$$$$$$$$ BATCH INSERT PROGRAM $$$$$$$$$$$$$$");
		return ;
	}

	private static void sort_temporary_heap_file(String dbname,int indexoption) 
	{	
		try
		{
			//sysdef = new SystemDefs(dbname,0,800,"Clock",indexoption);
			System.out.println("*** Opening existing database ***");
			TScan am = new TScan(sysdef.JavabaseDB.getTEMP_Triple_HF());
			TripleOrder sort_order = null;
			if(indexoption ==1) //SORT ON CONFIDENCE
			{
				sort_order = new TripleOrder(TripleOrder.Confidence);
			}
			else if(indexoption == 2)
			{
				sort_order = new TripleOrder(TripleOrder.SubjectConfidence);
			}
			else if(indexoption == 3)
			{
				sort_order = new TripleOrder(TripleOrder.ObjectConfidence);
			}
			else if(indexoption == 4)
			{
				sort_order = new TripleOrder(TripleOrder.PredicateConfidence);
			}
			else if(indexoption == 5)
			{
				sort_order = new TripleOrder(TripleOrder.SubjectPredicateObjectConfidence);
			}

			TripleSort tsort = new TripleSort(am, sort_order , 200);

			Triple triple = null;
			while((triple = tsort.get_next()) != null)
			{
				print_triple(triple);
				sysdef.JavabaseDB.insertTriple(triple.getTripleByteArray());
				System.out.println("*****************************");
			}
			tsort.close();
			
		}
		catch(Exception e)
		{
			System.out.println("TEMPORARY SORTING ERROR : INDEX OPTION ->" + indexoption );
		}
	}

}
