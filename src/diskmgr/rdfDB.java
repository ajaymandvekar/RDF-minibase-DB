/* File rdfDB.java */

package diskmgr;

import java.io.*;
import java.util.ArrayList;

import bufmgr.*;
import global.*;
import tripleheap.*;
import tripleheap.FileAlreadyDeletedException;
import tripleheap.InvalidSlotNumberException;
import labelheap.*;
import btree.*;

public class rdfDB extends DB implements GlobalConst {

	private TripleHeapfile TEMP_Triple_HF;		//TEMPORARY HEAP FILE FOR SORTING
    
	private TripleHeapfile Triple_HF; 	  		//Triples Heap file to store triples
	private LabelHeapfile Entity_HF; 	  		//Entity Heap file to store subjects/objects
	private LabelHeapfile Predicate_HF;   		//Predicates Heap file to store predicates

	private LabelBTreeFile Entity_BTree;  		//BTree Index file on Entity Heap file
	private LabelBTreeFile Predicate_BTree; 	//BTree Predicate file on Predicate Heap file
	private TripleBTreeFile Triple_BTree; 		//BTree Predicate file on Predicate Heap file

	private String curr_dbname; 				//RDF Database name

	private LabelBTreeFile dup_tree;        	//BTree file for duplicate subjects
	private LabelBTreeFile dup_Objtree;     	//BTree file for duplicate objects

	private int Total_Subjects = 0; 			//Total count of subjects in RDF
	private int Total_Objects = 0; 				//Total count of objects in RDF
	private int Total_Predicates = 0; 			//Total count of predicates in RDF
	private int Total_Triples = 0; 				//Total count of triples in RDF
	private int Total_Entities = 0;         	//Total count of entities in RDF
	
	
	private TripleBTreeFile Triple_BTreeIndex; 	//BTree file for the index options given
	// INDEX OPTIONS	
	//(1) BTree Index file on confidence
	//(2) BTree Index file on subject and confidence
	//(3) BTree Index file on object and confidence
	//(4) BTree Index file on predicate and confidence
	//(5) BTree Index file on subject

	
	public TripleHeapfile getTrpHandle() {
		// TODO Auto-generated method stub
		return Triple_HF;
	}

	public LabelHeapfile getEntityHandle() {
		// TODO Auto-generated method stub
		return Entity_HF;
	}
	public LabelHeapfile getPredicateHandle() {
		// TODO Auto-generated method stub
		return Predicate_HF;
	}
	
	public TripleHeapfile getTEMP_Triple_HF() {
		return TEMP_Triple_HF;
	}

	public TripleBTreeFile getTriple_BTreeIndex() 
	throws GetFileEntryException, PinPageException, ConstructPageException 
	{
		Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex");
		return Triple_BTreeIndex;
	}
	
	public TripleBTreeFile getTriple_BTree() 
	throws GetFileEntryException, PinPageException, ConstructPageException
	{
		Triple_BTree = new TripleBTreeFile(curr_dbname+"/tripleBT");
		return Triple_BTree;
	}
	/**
	 * Default Constructor
	 */
	public rdfDB() { }

	/**
	* Close RdfDB
	*/
	public void rdfcloseDB() 
	throws 	PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		try {

			if(Entity_BTree != null)
			{
				Entity_BTree.close();
				//Entity_BTree.destroyFile();
			}
			if(Predicate_BTree != null)
			{
				Predicate_BTree.close();
				//Predicate_BTree.destroyFile();
			}
			if(Triple_BTree != null)
			{
				Triple_BTree.close();
				//Triple_BTree.destroyFile();
			}
			if(dup_tree != null)
			{
				dup_tree.close(); 
				//dup_tree.destroyFile();
			}
			if(dup_Objtree != null)
			{
				dup_Objtree.close();
				//dup_Objtree.destroyFile();
			}
			if(Triple_BTreeIndex != null)
			{
				Triple_BTreeIndex.close();
				//Triple_BTreeIndex.destroyFile();
			}
			if(TEMP_Triple_HF != null && TEMP_Triple_HF != getTrpHandle())
			{
				TEMP_Triple_HF.deleteFile();
				//Triple_BTreeIndex.destroyFile();
			}
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Open an existing rdf database
	 * @param name Database name
	 */
	public void openrdfDB(String dbname,int type)
	{
		curr_dbname = new String(dbname);
		try 
		{
			openDB(dbname);
			rdfDB(type);
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	/**
	 * Create a new RDF database
	 * @param dbname Database name
	 * @param num_pages Num of pages to allocate for the database
	 * @param type different indexing types to use for the database
	 */
	public void openrdfDB(String dbname,int num_pages,int type)
	{
		curr_dbname = new String(dbname);
		try
		{
			openDB(dbname,num_pages);
			rdfDB(type);
		}
		catch(Exception e)
		{

			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	/**Constructor for the RDF database. 
	 * @param type is an integer denoting the different clus-tering and indexing strategies you will use for the rdf database.   
	 * @Note: Each RDF database contains:
	 * one TripleHeapFile to store the triples,
	 * one LabelHeapfile to store entity labels, 
	 * and another LabelHeapfile to store subject labels. 
	 * You can create as many btree index files as you want over these triple and label heap files
	 */
	public void rdfDB(int type) 
	{
		int keytype = AttrType.attrString;

		/** Initialize counter to zero **/ 
		PCounter.initialize();
		
		//Create TEMP TRIPLES heap file /TOFIX
		try
		{ 
			//System.out.println("Creating new TEMP triples heapfile");
			//TEMP_Triple_HF = new TripleHeapfile(Long.toString(System.currentTimeMillis()));
			TEMP_Triple_HF = new TripleHeapfile("tempresult");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		//Create TRIPLES heap file
		try
		{ 
			//System.out.println("Creating new triples heapfile");
			Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");

		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create ENTITES heap file: (Entity:Subject/Object)
		try
		{
			//System.out.println("Creating new entities heapfile");
			Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create PREDICATES heap file: (Predicates)
		try
		{
			//System.out.println("Creating new predicate heapfile");
			Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}


		//Create Entity Binary tree file
		try
		{
			//System.out.println("Creating new entity Binary Tree file");
			Entity_BTree = new LabelBTreeFile(curr_dbname+"/entityBT",keytype,255,1);
			Entity_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create Predicate Binary tree file
		try
		{
			//System.out.println("Creating new Predicate Binary Tree file");
			Predicate_BTree = new LabelBTreeFile(curr_dbname+"/predicateBT",keytype,255,1);
			Predicate_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		//Create Triple Binary tree file
		try
		{
			//System.out.println("Creating new Triple Binary Tree file");
			Triple_BTree = new TripleBTreeFile(curr_dbname+"/tripleBT",keytype,255,1);
			Triple_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			//System.out.println("Creating new Label Binary Tree file for checking duplicate subjects");
			dup_tree = new LabelBTreeFile(curr_dbname+"/dupSubjBT",keytype,255,1);
			dup_tree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			//System.out.println("Creating new Label Binary Tree file for checking duplicate objects");
			dup_Objtree = new LabelBTreeFile(curr_dbname+"/dupObjBT",keytype,255,1);
			dup_Objtree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Now create btree index files as per the index option
		try
		{
			//System.out.println("Creating Triple Binary Tree file for given index option");
			Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex",keytype,255,1);
			Triple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error creating B tree index for given index option"+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

	/**
	 *  Get count of Triples in RDF DB
	 *  @return int number of Triples
	 */ 
	public int getTripleCnt()
	{	
		try
		{
			Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");
			Total_Triples = Triple_HF.getRecCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Total_Triples;
	}

	/**
	 *  Get count of Predicates(unique) in RDF DB
	 *  @return int number of distinct Predicates
	 */ 
	public int getPredicateCnt()
	{
		try
		{
			Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
			Total_Predicates = Predicate_HF.getRecCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Total_Predicates; 
	}

	/**
	 *  Get count of Subjects(unique) in RDF DB
	 *  @return int number of distinct subjects
	 */ 
	public int getSubjectCnt()
	{
        Total_Subjects = 0;
        KeyDataEntry entry = null;
        KeyDataEntry dup_entry = null;
        try
        {
                Triple_BTree = new TripleBTreeFile(curr_dbname+"/tripleBT");
                int keytype = AttrType.attrString;
                dup_tree = new LabelBTreeFile(curr_dbname+"/dupSubjBT");
                //Start Scanning Btree to check if  predicate already present
                TripleBTFileScan scan = Triple_BTree.new_scan(null,null);
                do
                {
                        entry = scan.get_next();
                        if(entry != null)
                        {
                                String label = ((StringKey)(entry.key)).getKey();
                                String[] temp;
                                /* delimiter */
                                String delimiter = ":";
                                /* given string will be split by the argument delimiter provided. */
                                temp = label.split(delimiter);
                                String subject = temp[0] + temp[1];
                                //Start Scaning Label Btree to check if subject already present
                                KeyClass low_key = new StringKey(subject);
                                KeyClass high_key = new StringKey(subject);
                                LabelBTFileScan dup_scan = dup_tree.new_scan(low_key,high_key);
                                dup_entry = dup_scan.get_next();
                                if(dup_entry == null)
                                {
                                        //subject not present in btree, hence insert
                                        dup_tree.insert(low_key,new LID(new PageId(Integer.parseInt(temp[1])),Integer.parseInt(temp[0])));
        
                                }
                                dup_scan.DestroyBTreeFileScan();
                                        
                        }

                }while(entry!=null);
                scan.DestroyBTreeFileScan();
                Triple_BTree.close();
                
                KeyClass low_key = null;
                KeyClass high_key = null;
                LabelBTFileScan dup_scan = dup_tree.new_scan(low_key,high_key);
                do
                {
                        dup_entry = dup_scan.get_next();
                        if(dup_entry!=null)
                                Total_Subjects++;

                }while(dup_entry!=null);
                dup_scan.DestroyBTreeFileScan();
                dup_tree.close();
        }
        catch(Exception e)
        {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        return Total_Subjects;
	}

    /**
     *  Get count of Entities(unique) in RDF DB
     *  @return int number of distinct Entities
     */ 
    public int getEntityCnt()
    {
            try
            {
                    Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
                    Total_Entities = Entity_HF.getRecCnt();
            }
            catch (Exception e) 
            {
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }
            return Total_Entities; 
    }
    
	/**
	 *  Get count of Objects(unique) in RDF DB
	 *  @return int number of distinct objects
	 */ 
	public int getObjectCnt()
	{
        Total_Objects = 0;
        KeyDataEntry entry = null;
        KeyDataEntry dup_entry = null;
        try
        {
                Triple_BTree = new TripleBTreeFile(curr_dbname+"/tripleBT");
                int keytype = AttrType.attrString;
                dup_Objtree = new LabelBTreeFile(curr_dbname+"/dupObjBT");
                //Start Scaning Btree to check if  predicate already present
                TripleBTFileScan scan = Triple_BTree.new_scan(null,null);
                do
                {
                        entry = scan.get_next();
                        if(entry != null)
                        {
                                String label = ((StringKey)(entry.key)).getKey();
                                String[] temp;
                                /* delimiter */
                                String delimiter = ":";
                                /* given string will be split by the argument delimiter provided. */
                                temp = label.split(delimiter);
                                String object = temp[4] + temp[5];
                                //Start Scaning Label Btree to check if subject already present
                                KeyClass low_key = new StringKey(object);
                                KeyClass high_key = new StringKey(object);
                                LabelBTFileScan dup_scan = dup_Objtree.new_scan(low_key,high_key);
                                dup_entry = dup_scan.get_next();
                                if(dup_entry == null)
                                {
                                        //subject not present in btree, hence insert
                                        dup_Objtree.insert(low_key,new LID(new PageId(Integer.parseInt(temp[4])),Integer.parseInt(temp[5])));
        
                                }
                                dup_scan.DestroyBTreeFileScan();
                                        
                        }

                }while(entry!=null);
                scan.DestroyBTreeFileScan();
                Triple_BTree.close();
                
                KeyClass low_key = null;
                KeyClass high_key = null;
                LabelBTFileScan dup_scan = dup_Objtree.new_scan(low_key,high_key);
                do
                {
                        dup_entry = dup_scan.get_next();
                        if(dup_entry!=null)
                                Total_Objects++;

                }while(dup_entry!=null);
                dup_scan.DestroyBTreeFileScan();
                dup_Objtree.close();
        }
        catch(Exception e)
        {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        return Total_Objects;
	}

	/**
	 * Insert a entity into the EntityHeapFIle
	 * @param Entitylabel String representing Subject/Object
	 */
	public EID insertEntity(String EntityLabel) 
	{
	    int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        //Open ENTITY BTree Index file
        try
        {
                Entity_BTree = new LabelBTreeFile(curr_dbname+"/entityBT");
                //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

                LID lid = null;
                KeyClass low_key = new StringKey(EntityLabel);
                KeyClass high_key = new StringKey(EntityLabel);
                KeyDataEntry entry = null;

                //Start Scaning Btree to check if entity already present
                LabelBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry!=null)
                {
                        if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
                        {
                                //return already existing EID ( convert lid to EID)
                                lid =  ((LabelLeafData)entry.data).getData();
                                entityid = lid.returnEID();
                                scan.DestroyBTreeFileScan();
                                Entity_BTree.close();
                                return entityid;
                        }
                }

                scan.DestroyBTreeFileScan();
                //Insert into Entity HeapFile
                lid = Entity_HF.insertRecord(EntityLabel.getBytes());   

                //Insert into Entity Btree file key,lid
                Entity_BTree.insert(key,lid); 

                entityid = lid.returnEID();
                Entity_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error inserting entity ");
                e.printStackTrace();
        }

        return entityid; //Return EID
	}

	/**
	 * Delete a entity into the EntityHeapFile
	 * @param Entitylabel String representing Subject/Object
	 * @return boolean success when deleted else false
	 */
	public boolean deleteEntity(String EntityLabel)
	{
        boolean success = false;
        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        //Open ENTITY BTree Index file
        try
        {
                Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
                Entity_BTree = new LabelBTreeFile(curr_dbname+"/entityBT");
                //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

                LID lid = null;
                KeyClass low_key = new StringKey(EntityLabel);
                KeyClass high_key = new StringKey(EntityLabel);
                KeyDataEntry entry = null;

                //Start Scaning Btree to check if entity already present
                LabelBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry!=null)
                {
                        if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
                        {
                                //System.out.println(((StringKey)(entry.key)).getKey());        
                                lid =  ((LabelLeafData)entry.data).getData();
                                success = Entity_HF.deleteRecord(lid) & Entity_BTree.Delete(low_key,lid);
                        }
                }
                scan.DestroyBTreeFileScan();
                Entity_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error deleting entity " + e);
                e.printStackTrace();
        }
        return success;
	}


	/**
	 * Insert a entity into the EntityHeapFile
	 * @param Entitylabel String representing Subject/Object
	 */
	public PID insertPredicate(String PredicateLabel)
	{
        PID predicateid = null;
        LID lid = null;

        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(PredicateLabel);

        //Open PREDICATE BTree Index file
        try
        {
                Predicate_BTree = new LabelBTreeFile(curr_dbname+"/predicateBT"); 
                //LabelBT.printAllLeafPages(Predicate_BTree.getHeaderPage());
                KeyClass low_key = new StringKey(PredicateLabel);
                KeyClass high_key = new StringKey(PredicateLabel);
                KeyDataEntry entry = null;

                //Start Scaning Btree to check if  predicate already present
                LabelBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry != null)
                {
                        if(PredicateLabel.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                        {
                                //return already existing EID ( convert lid to EID)
                                predicateid = ((LabelLeafData)(entry.data)).getData().returnPID();
                                scan.DestroyBTreeFileScan();
                                Predicate_BTree.close(); //Close the Predicate Btree file
                                return predicateid;
                        }
                }
                scan.DestroyBTreeFileScan();
                //Insert into Predicate HeapFile
                lid = Predicate_HF.insertRecord(PredicateLabel.getBytes());
                //Insert into Predicate Btree file key,lid
                Predicate_BTree.insert(key,lid); 
                predicateid = lid.returnPID();
                Predicate_BTree.close(); //Close the Predicate Btree file
        }
        catch(Exception e)
        {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        return predicateid;
	}

	public boolean deletePredicate(String PredicateLabel)
	{
        boolean success = false;
        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(PredicateLabel);
        EID predicateid = null;

        //Open ENTITY BTree Index file
        try
        {
                Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
                Predicate_BTree = new LabelBTreeFile(curr_dbname+"/predicateBT");
                //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

                LID lid = null;
                KeyClass low_key = new StringKey(PredicateLabel);
                KeyClass high_key = new StringKey(PredicateLabel);
                KeyDataEntry entry = null;

                //Start Scanning BTree to check if entity already present
                LabelBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry!=null)
                {
                        if(PredicateLabel.equals(((StringKey)(entry.key)).getKey()))
                        {
                                //System.out.println(((StringKey)(entry.key)).getKey());        
                                lid =  ((LabelLeafData)entry.data).getData();
                                success = Predicate_HF.deleteRecord(lid) & Predicate_BTree.Delete(low_key,lid);
                        }
                }
                scan.DestroyBTreeFileScan();
                Predicate_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error deleting predicate " + e);
                e.printStackTrace();
        }
        
        return success;
	}

	public TID insertTriple(byte[] triplePtr)
	throws Exception
	{
		  TID tripleid;
          TID tid = null;
          try
          {
                  //Open Triple BTree Index file
                  Triple_BTree = new TripleBTreeFile(curr_dbname+"/tripleBT"); 
                  //TripleBT.printAllLeafPages(Triple_BTree.getHeaderPage());
                  int sub_slotNo = Convert.getIntValue(0,triplePtr);
                  int sub_pageNo = Convert.getIntValue(4,triplePtr);
                  int pred_slotNo = Convert.getIntValue(8,triplePtr); 
                  int pred_pageNo = Convert.getIntValue(12,triplePtr);
                  int obj_slotNo = Convert.getIntValue(16,triplePtr);
                  int obj_pageNo = Convert.getIntValue(20,triplePtr);
                  double confidence =Convert.getDoubleValue(24,triplePtr);
                  String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
                  KeyClass low_key = new StringKey(key);
                  KeyClass high_key = new StringKey(key);
                  KeyDataEntry entry = null;

                  //Start Scaning Btree to check if  predicate already present
                  TripleBTFileScan scan = Triple_BTree.new_scan(low_key,high_key);
                  entry = scan.get_next();
                  if(entry != null)
                  {
                          //System.out.println("Duplicate Triple found : " + ((StringKey)(entry.key)).getKey());
                          if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                          {
                                  //return already existing TID 
                                  tripleid = ((TripleLeafData)(entry.data)).getData();
                                  Triple record = Triple_HF.getRecord(tripleid);
                                  double orig_confidence = record.getConfidence();
                                  if(orig_confidence > confidence)
                                  {
                                          Triple newRecord = new Triple(triplePtr,0,32);
                                          Triple_HF.updateRecord(tripleid,newRecord);
                                  }       
                                  scan.DestroyBTreeFileScan();
                                  Triple_BTree.close();
                                  return tripleid;
                          }
                  }

                  //insert into triple heap file
                  //System.out.println("("+triplePtr+")");
                  tid= Triple_HF.insertTriple(triplePtr);

                  //System.out.println("Inserting triple key : "+ key + "tid : " + tid);
                  //insert into triple btree
                  Triple_BTree.insert(low_key,tid); 
  
                  scan.DestroyBTreeFileScan();
                  Triple_BTree.close();
          }
          catch(Exception e)
          {
                  System.err.println ("*** Error inserting triple record " + e);
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
          }

          return tid;
	}

	public boolean deleteTriple(byte[] triplePtr)
	{
        boolean success = false;
        TID tripleid = null;
        try
        {
                //Open Triple BTree Index file
                Triple_BTree = new TripleBTreeFile(curr_dbname+"/tripleBT"); 
                //TripleBT.printAllLeafPages(Triple_BTree.getHeaderPage());
                int sub_slotNo = Convert.getIntValue(0,triplePtr);
                int sub_pageNo = Convert.getIntValue(4,triplePtr);
                int pred_slotNo = Convert.getIntValue(8,triplePtr); 
                int pred_pageNo = Convert.getIntValue(12,triplePtr);
                int obj_slotNo = Convert.getIntValue(16,triplePtr);
                int obj_pageNo = Convert.getIntValue(20,triplePtr);
                double confidence =Convert.getDoubleValue(24,triplePtr);
                String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
                //System.out.println(key);
                KeyClass low_key = new StringKey(key);
                KeyClass high_key = new StringKey(key);
                KeyDataEntry entry = null;

                //Start Scaning Btree to check if  predicate already present
                TripleBTFileScan scan = Triple_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry != null)
                {
                        //System.out.println("Triple found : " + ((StringKey)(entry.key)).getKey());
                        if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                        {
                                //return already existing TID 
                                tripleid = ((TripleLeafData)(entry.data)).getData();
                                if(tripleid!=null)
                                success = Triple_HF.deleteRecord(tripleid); 
                        }
                }
                scan.DestroyBTreeFileScan();
                if(entry!=null)
                {
                if(low_key!=null && tripleid!=null)
                success = success & Triple_BTree.Delete(low_key,tripleid);
                }

                Triple_BTree.close();
                
        }
        catch(Exception e)
        {
                System.err.println ("*** Error deleting triple record " + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        
        return success;
	}

	public Stream openStream(String dbname,int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter)
	{
		Stream streamObj= null;
		try {
			streamObj = new Stream( dbname,orderType, subjectFilter,  predicateFilter, objectFilter, confidenceFilter);
		} catch (tripleheap.InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTripleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (THFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (THFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (THFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*TODO*/

		return streamObj;

	}

	public Stream openStreamWithoutSort(String dbname, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter)
	{
		Stream streamObj= null;
		try {
			streamObj = new Stream( dbname, subjectFilter,  predicateFilter, objectFilter, confidenceFilter);
		} catch (tripleheap.InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTripleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (THFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (THFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (THFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*TODO*/

		return streamObj;

	}
	
	public void createIndex(int type)
    {

            switch(type)
            {
                    case 1:
                    createIndex1();
                    break;

                    case 2:
                    createIndex2();
                    break;
                    
                    case 3:
                    createIndex3();
                    break;


                    case 4:
                    createIndex4();
                    break;

                    case 5:
                    createIndex5();
                    break;  
                    
            }
    }

    public void createIndex1()
    {
            //Unclustered BTree on confidence using sorted Heap File
            try
            {
                    //destroy existing index first
                    if(Triple_BTreeIndex != null)
                    {
                            Triple_BTreeIndex.close();
                            Triple_BTreeIndex.destroyFile();
                            destroyIndex(curr_dbname+"/Triple_BTreeIndex");
                    }

                    //create new
                    int keytype = AttrType.attrString;
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex",keytype,255,1);
                    Triple_BTreeIndex.close();
                    
                    //scan sorted heap file and insert into btree index
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex"); 
                    Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");
                    TScan am = new TScan(Triple_HF);
                    Triple triple = null;
                    TID tid = new TID();
                    double confidence = 0.0;
                    while((triple = am.getNext(tid)) != null)
                    {
                            confidence = triple.getConfidence();
                            String temp = Double.toString(confidence);
                            KeyClass key = new StringKey(temp);
                            //System.out.println("Inserting into Btree key"+ temp + " tid "+tid);
                            Triple_BTreeIndex.insert(key,tid); 
                            //System.out.println("Inserting into Btree key"+ temp + " tid "+tid);
                            
                    }
                    /*
                    TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
                    KeyDataEntry entry = null;
                    while((entry = scan.get_next())!= null)
                    {
                            System.out.println("Triple found : " + ((StringKey)(entry.key)).getKey());
                    }
                    scan.DestroyBTreeFileScan();
                    */
                    am.closescan();
                    Triple_BTreeIndex.close();
            }
            catch(Exception e)
            {
                    System.err.println ("*** Error creating Index for option1 " + e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }

    }


    public void createIndex2()
    {
            //Unclustered BTree Index file on subject and confidence
            try
            {
                    //destroy existing index first
                    if(Triple_BTreeIndex != null)
                    {
                            Triple_BTreeIndex.close();
                            Triple_BTreeIndex.destroyFile();
                            destroyIndex(curr_dbname+"/Triple_BTreeIndex");
                    }

                    //create new
                    int keytype = AttrType.attrString;
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex",keytype,255,1);
                    Triple_BTreeIndex.close();
                    
                    //scan sorted heap file and insert into btree index
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex"); 
                    Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");
                    Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
                    TScan am = new TScan(Triple_HF);
                    Triple triple = null;
                    TID tid = new TID();
                    double confidence = 0.0;
                    while((triple = am.getNext(tid)) != null)
                    {
                            confidence = triple.getConfidence();
                            String temp = Double.toString(confidence);
                            Label subject = Entity_HF.getRecord(triple.getSubjectID().returnLID());
                            //System.out.println("Subject--> "+subject.getLabelKey());
                            KeyClass key = new StringKey(subject.getLabelKey()+":"+temp);
                            //System.out.println("Inserting into Btree key"+ subject.getLabelKey() + ":" + temp + " tid "+tid);
                            Triple_BTreeIndex.insert(key,tid); 
                    }
                    /*
                    TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
                    KeyDataEntry entry = null;
                    while((entry = scan.get_next())!= null)
                    {
                            System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
                    }
                    scan.DestroyBTreeFileScan();
                    */
                    am.closescan();
                    Triple_BTreeIndex.close();
            }
            catch(Exception e)
            {
                    System.err.println ("*** Error creating Index for option2 " + e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }

    }

    public void createIndex3()
    {
            //Unclustered BTree Index file on object and confidence
            try
            {
                    //destroy existing index first
                    if(Triple_BTreeIndex != null)
                    {
                            Triple_BTreeIndex.close();
                            Triple_BTreeIndex.destroyFile();
                            destroyIndex(curr_dbname+"/Triple_BTreeIndex");
                    }

                    //create new
                    int keytype = AttrType.attrString;
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex",keytype,255,1);
                    Triple_BTreeIndex.close();
                    
                    //scan sorted heap file and insert into btree index
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex"); 
                    Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");
                    Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
                    TScan am = new TScan(Triple_HF);
                    Triple triple = null;
                    TID tid = new TID();
                    double confidence = 0.0;
                    while((triple = am.getNext(tid)) != null)
                    {
                            confidence = triple.getConfidence();
                            String temp = Double.toString(confidence);
                            Label object = Entity_HF.getRecord(triple.getObjectID().returnLID());
                            //System.out.println("Subject--> "+subject.getLabelKey());
                            KeyClass key = new StringKey(object.getLabelKey()+":"+temp);
                            //System.out.println("Inserting into Btree key"+ object.getLabelKey() + ":" + temp + " tid "+tid);
                            Triple_BTreeIndex.insert(key,tid); 
                    }
                    /*
                    TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
                    KeyDataEntry entry = null;
                    while((entry = scan.get_next())!= null)
                    {
                            System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
                    }
                    scan.DestroyBTreeFileScan();
                    */
                    am.closescan();
                    Triple_BTreeIndex.close();
            }
            catch(Exception e)
            {
                    System.err.println ("*** Error creating Index for option2 " + e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }

    }

    public void createIndex4()
    {
            //Unclustered BTree Index file on predicate and confidence
            try
            {
                    //destroy existing index first
                    if(Triple_BTreeIndex != null)
                    {
                            Triple_BTreeIndex.close();
                            Triple_BTreeIndex.destroyFile();
                            destroyIndex(curr_dbname+"/Triple_BTreeIndex");
                    }

                    //create new
                    int keytype = AttrType.attrString;
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex",keytype,255,1);
                    Triple_BTreeIndex.close();
                    
                    //scan sorted heap file and insert into btree index
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex"); 
                    Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");
                    Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
                    TScan am = new TScan(Triple_HF);
                    Triple triple = null;
                    TID tid = new TID();
                    double confidence = 0.0;
                    while((triple = am.getNext(tid)) != null)
                    {
                            confidence = triple.getConfidence();
                            String temp = Double.toString(confidence);
                            Label predicate = Predicate_HF.getRecord(triple.getPredicateID().returnLID());
                            //System.out.println("Subject--> "+subject.getLabelKey());
                            KeyClass key = new StringKey(predicate.getLabelKey()+":"+temp);
                            //System.out.println("Inserting into Btree key"+ predicate.getLabelKey() + ":" + temp + " tid "+tid);
                            Triple_BTreeIndex.insert(key,tid); 
                    }
                    /*
                    TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
                    KeyDataEntry entry = null;
                    while((entry = scan.get_next())!= null)
                    {
                            System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
                    }
                    scan.DestroyBTreeFileScan();
                    */
                    am.closescan();
                    Triple_BTreeIndex.close();
            }
            catch(Exception e)
            {
                    System.err.println ("*** Error creating Index for option2 " + e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }


    }

    public void createIndex5()
    {
            //Unclustered BTree Index file on subject
            try
            {
                    //destroy existing index first
                    if(Triple_BTreeIndex != null)
                    {
                            Triple_BTreeIndex.close();
                            Triple_BTreeIndex.destroyFile();
                            destroyIndex(curr_dbname+"/Triple_BTreeIndex");
                            
                    }

                    //create new
                    int keytype = AttrType.attrString;
                    Triple_BTreeIndex = new TripleBTreeFile(curr_dbname+"/Triple_BTreeIndex",keytype,255,1);
                    
                    //scan sorted heap file and insert into btree index
                    Triple_HF = new TripleHeapfile(curr_dbname+"/tripleHF");
                    Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
                    TScan am = new TScan(Triple_HF);
                    Triple triple = null;
                    TID tid = new TID();
                    KeyDataEntry entry = null;
                    TripleBTFileScan scan = null;
                    
                    /*TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
                    while((entry = scan.get_next())!= null)
                    {
                            System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
                    }
                    scan.DestroyBTreeFileScan();
                    */

                    while((triple = am.getNext(tid)) != null)
                    {
                            Label subject = Entity_HF.getRecord(triple.getSubjectID().returnLID());
                            KeyClass key = new StringKey(subject.getLabelKey());
                       //     entry = null;

                            //Start Scanning Btree to check if subject already present
                       //     scan = Triple_BTreeIndex.new_scan(key,key);
                       //     entry = scan.get_next();
                       //     if(entry == null)
                       //     {
                                    Triple_BTreeIndex.insert(key,tid); 
                                    //System.out.println("Inserting into Btree key"+ subject.getLabelKey() + " tid "+tid);
                       //     }
                       //     else
                       //             System.out.println("Duplicate subject found "+ subject.getLabelKey() + " tid "+tid);
                                    
                      //      scan.DestroyBTreeFileScan();
                    }
                    /*
                    scan = Triple_BTreeIndex.new_scan(null,null);
                    entry = null;
                    while((entry = scan.get_next())!= null)
                    {
                            System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
                    }
                    scan.DestroyBTreeFileScan();
                    */
                    am.closescan();
                    Triple_BTreeIndex.close();
            }
            catch(Exception e)
            {
                    System.err.println ("*** Error creating Index for option5 " + e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }
    }

    private void destroyIndex(String filename)
    {
            try
            {
                    if(filename != null)
                    {
                    		
                            TripleBTreeFile bfile = new TripleBTreeFile(filename);
                            
                            TripleBTFileScan scan = bfile.new_scan(null,null);
                            TID tid = null;
                            KeyDataEntry entry = null;
                            ArrayList<KeyClass> keys = new ArrayList<KeyClass>();                   
                            ArrayList<TID> tids = new ArrayList<TID>();
                            int count = 0;                  

                            while((entry = scan.get_next())!= null)
                            {
                                    tid =  ((TripleLeafData)entry.data).getData();
                                    keys.add(entry.key);
                                    tids.add(tid);
                                    count++;
                            }
                            scan.DestroyBTreeFileScan();

                            for(int i = 0; i < count ;i++)
                            {
                                    //System.out.println("Deleting record having Key : " + keys.get(i) + " TID " + tids.get(i));
                                    bfile.Delete(keys.get(i),tids.get(i));
                            }

                            bfile.close();  

                    }
            }
            catch(GetFileEntryException e1)
            {
            		System.out.println("Firsttime No index present.. Expected");
            }
            catch(Exception e)
            {
                    System.err.println ("*** Error destroying Index " + e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }

    }

}//end of rdfDB class
