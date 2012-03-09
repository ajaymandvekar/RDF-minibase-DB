package tests;

import diskmgr.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.Sort;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import tripleheap.*;
import labelheap.*;
import tripleiterator.*;
import basicpattern.*;
import basicpatterniterator.*;

public class QueryProgramJoin {
	public static SystemDefs sysdef = null;
	static String dbname = null;   //Database name 
	static String Subject = null;
	static String Object = null;
	static String Predicate = null;
	static String Confidence = null;
	static int indexoption = 1;    //Index option
	static EID entityobjectid = new EID();
	static EID entitysubjectid = new EID();
	static EID entitypredicateid = new EID();
	static TripleHeapfile UNSORTED_TRIPLES = null;
	boolean exists = false;
	public static double confidence = -99.0;
	public static int num_of_buf = 200;
	
	public static String queryfile = null;
	public static int FJNP = 0, SJNP =0, FJONO = 0, SJONO = 0, FORS = 0, SORS = 0, FORO=0,SORO=0;
	public static String FRSF=null,SRSF=null,FRPF=null,SRPF=null,FROF=null,SROF=null;
	public static double FRCF = -99.0,SRCF = -99.0;
	public static int[] FLONP = null, SLONP = null;
	public static List<Integer> SLONP_list = new ArrayList<Integer>();
	public static List<Integer> FLONP_list = new ArrayList<Integer>();

	public static int bporder=0;
	public static int node_position = 0,num_of_sort_pages = 0;
	
	public static String get_sort_order()
	{
		
		switch(indexoption)
		{
		case 1:	
			return new String(" Sorting by Subject-Predicate-Object-Confidence");

		case 2:
			return new String(" Sorting by Predicate-Subject-Object-Confidence");
			
		case 3:	
			return new String(" Sorting by Subject-Confidence");

		case 4:
			return new String(" Sorting by Predicate-Confidence");

		case 5:	
			return new String(" Sorting by Object-Confidence");

		case 6:	
			return new String(" Sorting by Confidence");
			
		default:
			return new String(" Sorting by Subject-Predicate-Object-Confidence");
		}
		
	}
	

	public static void parse_query_file() throws IOException
	{
		FileInputStream fstream = new FileInputStream(queryfile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine = new String("");
		String query = new String("");
		while ((strLine = br.readLine()) != null)   
		{
			query = query.concat(strLine);
		}
		
		String delims = "[()\\[\\]]+";
		String[] tokens = query.split(delims);
		for(String a:tokens)
		{
			//System.out.println(a.trim());
		}
		
		if(tokens.length == 7 && tokens[0].contains("S") && tokens[1].contains("J") && tokens[2].contains("J"))
		{
			delims = "[,]";
			String[] str = tokens[3].split(delims); //SF1,PF1,OF1,CF1
			if(str.length == 4)
			{
				
				Subject = new String(str[0]);
				Predicate = new String(str[1]);
				Object = new String(str[2]);
				Confidence = new String(str[3]);
				if(Confidence.compareToIgnoreCase("null") != 0)
				{
					confidence = Double.parseDouble(Confidence);
				}
				System.out.println(Subject+ " " + Predicate + " " + Object + " "+ confidence);
			}
			else
			{
				System.out.println("***ERROR in query file format***");
				return ;
			}
			
			str = tokens[4].split(delims); //,JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
			if(str.length == 10)
			{
				int i = 0;
				FJNP = Integer.parseInt(str[1]);
				FJONO = Integer.parseInt(str[2]);
				FRSF = new String(str[3]);
				FRPF = new String(str[4]);
				FROF = new String(str[5]);
				
				if(str[6].compareTo("null") != 0)
				{
					FRCF = Double.parseDouble(str[6]);
				}
				
				String[] lonp_str = str[7].split(":");
				for(String lonp:lonp_str)
				{
					FLONP_list.add(Integer.parseInt(lonp));
					i++;
				}
				
				FORS = Integer.parseInt(str[8]);
				FORO = Integer.parseInt(str[9]);
				
				System.out.println(FJNP+ " " + FJONO + " " + FRSF + " "+ FRPF + " "+ FRCF + " "+ FORS + " " + FORO);
			}
			else
			{
				System.out.println("***ERROR in query file format***");
				return ;
			}
			
			str = tokens[5].split(delims); //,JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
			if(str.length == 10)
			{
				int i = 0;
				SJNP = Integer.parseInt(str[1]);
				SJONO = Integer.parseInt(str[2]);
				SRSF = new String(str[3]);
				SRPF = new String(str[4]);
				SROF = new String(str[5]);
				if(str[6].compareTo("null") != 0)
				{
					SRCF = Double.parseDouble(str[6]);
				}
				
				String[] lonp_str = str[7].split(":");
				for(String lonp:lonp_str)
				{
					SLONP_list.add(Integer.parseInt(lonp));
					i++;
				}
				SORS = Integer.parseInt(str[8]);
				SORO = Integer.parseInt(str[9]);
				
				System.out.println(SJNP+ " " + SJONO + " " + SRSF + " "+ SRPF + " "+ SRCF + " "+ SORS + " " + SORO);
			}
			else
			{
				System.out.println("***ERROR in query file format***");
				return ;
			}
			
			str = tokens[6].split(delims);
			System.out.println(Integer.parseInt(str[0].trim())+" " + Integer.parseInt(str[1].trim())+" " + Integer.parseInt(str[2].trim()));
			if(str.length == 3)
			{
				num_of_sort_pages = Integer.parseInt(str[2].trim());
				node_position = Integer.parseInt(str[1].trim());
				bporder = Integer.parseInt(str[0].trim());
			}
			
		}
		else
		{
			System.out.println("***ERROR in query file format***");
			return ;
		}
		
		/*if(num_of_buf < 50)
		{
			System.out.println("Num of bufs too low.. setting it to 60");
			num_of_buf = 60;
		}*/

	}
	
	/**
	 * @param args RDFDBNAME QUERYFILE NUMBUF
	 * @throws Exception
	 */
	public static void main(String[] args)
	throws Exception 
	{
		if(args.length == 3 )   //Check if the arguments are RDFDBNAME QUERYFILE NUMBUF
		{
			
			dbname = new String("/tmp/" + args[0]);
			queryfile = new String(args[1]);
			num_of_buf = Integer.parseInt(args[2]);
			String[] dbstoken = args[0].split("[_]");
			indexoption = Integer.parseInt(dbstoken[1]);
			
			parse_query_file();
			
			File dbfile = new File(dbname); //Check if database already exist
			if(dbfile.exists())
			{
				//Database already present just open it
				sysdef = new SystemDefs(dbname,0,num_of_buf,"Clock",indexoption);
				System.out.println("\n"+get_sort_order());
			}
			else
			{	
				System.out.println("*** Database does not exist ***");
				return;
			}

			/** Get the matching raw file contents**/
			Stream s = SystemDefs.JavabaseDB.openStreamWithoutSort(dbname, Subject, Predicate, Object, confidence);
			
			//RAW FILE GENERATION
			System.out.println("\n\n***************Printing the raw file results***************");
			Heapfile RAW_BP_FILE = new Heapfile("RAW_BP_FILE");
			BasicPattern bp = null;
			TID tid = null;
			while((bp = s.getNextBasicPatternFromTriple(tid))!=null)
			{
				bp.print();
				RAW_BP_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
			}
			if(s!=null)
			{
				s.closeStream();
			}
			
			if(RAW_BP_FILE.getRecCnt() > 0)
			{
				//FIRST JOIN OPERATION
				System.out.println("\n\n***************Printing the first join results***************");
				Heapfile FIRST_JOIN_FILE = new Heapfile("FIRST_JOIN_FILE");
				BPFileScan newscan = new BPFileScan("RAW_BP_FILE",3);
				FLONP = new int[FLONP_list.size()];
				for(int i = 0; i < FLONP_list.size(); i++) 
				{
					FLONP[i] = FLONP_list.get(i);
				}
				BP_Triple_Join bpjoin = new BP_Triple_Join(num_of_buf, 3, newscan , FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
				bp = bpjoin.get_next();
				int fldcnt = 0;
				while(bp != null)
				{
					bp.print();
					fldcnt = bp.noOfFlds();
					FIRST_JOIN_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
					bp = bpjoin.get_next();
					
				}
				bpjoin.close();
				
				RAW_BP_FILE.deleteFile();
				System.out.println(fldcnt);
				//SECOND JOIN OPERATION
				int fldCount=0;
				Heapfile SECOND_JOIN_FILE = new Heapfile("SECOND_JOIN_FILE");
				if(fldcnt>0)
				{
					System.out.println("\n\n***************Printing the second join results***************");
					newscan = new BPFileScan("FIRST_JOIN_FILE",fldcnt);
					SLONP = new int[SLONP_list.size()];
					for(int i = 0; i < SLONP_list.size(); i++) 
					{
						SLONP[i] = (Integer) SLONP_list.get(i);
					}
					bpjoin = new BP_Triple_Join(num_of_buf, fldcnt,newscan , SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
					BasicPattern bp1 = bpjoin.get_next();
					fldCount = bp1.noOfFlds();
					while(bp1 != null)
					{
						bp1.print();
						SECOND_JOIN_FILE.insertRecord(bp1.getTuplefromBasicPattern().getTupleByteArray());
						bp1 = bpjoin.get_next();
					}
					bpjoin.close();
				}
				
				FIRST_JOIN_FILE.deleteFile();
				System.out.println(fldCount);
				//SORT
				if(SECOND_JOIN_FILE.getRecCnt() > 0)
				{
						System.out.println("\n\n***************Printing SORTED results***************");
						BPFileScan fscan = null;	    
					    try {
					      fscan = new BPFileScan("SECOND_JOIN_FILE", fldCount);
					    }
					    catch (Exception e) {
					      e.printStackTrace();
					    }
					    
					    BPSort sort = null;
					    BPOrder order = new BPOrder(bporder);
					    try {
					      sort = new BPSort(fscan, order, node_position, num_of_sort_pages);
					    }
					    catch (Exception e) {
					      e.printStackTrace();
					    }
					    		    
					    try {
					    	while((bp = sort.get_next()) != null) {
					    		bp.print();
					    	}
					    }
					    catch (Exception e) {
					      e.printStackTrace(); 
					    }
					    System.out.println("** SORTING DONE **");
					    sort.close();
					    
				}
				//CLEANUP OF THE FILES
				
				SECOND_JOIN_FILE.deleteFile();
			}
			
		}
		else
		{
			System.out.println("*** Usage:QueryProgramJoin RDFDBNAME QUERYFILE NUMBUF***");
			return;
		}

		System.out.println("**************************************");
		System.out.println("Total Page Writes "+ PCounter.wcounter);
		System.out.println("Total Page Reads "+ PCounter.rcounter);
		SystemDefs.close();
	}
	
	
}
