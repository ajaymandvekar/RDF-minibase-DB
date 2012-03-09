package diskmgr;

public class PCounter {
	public static int rcounter;
	public static int wcounter;

	/*
	Initialize the read and write page counters to 0*/
	
	public static void initialize(){
		rcounter = 0;
		wcounter = 0;
	}

	/*
        Increment the read page counter
        */
	public static void readIncrement(){
		rcounter++;
	}

	/*
	Increment the write page counter
        */
	public static void writeIncrement(){
		wcounter++;
	}
}
