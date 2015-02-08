package tastingwine;

import java.io.*;
import java.util.*;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class TastingWine {
	static final String inputFilename = "res/person_wine_3.txt"; /* Path to question input file */
	static final String resultFilename = "res/result3.txt"; /* Path to result file */
	
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(inputFilename));
		System.out.println("Going to open mongoDB Connection....");
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		DB db = mongoClient.getDB("mydb2");
		DBCollection wishlistcollection = db.getCollection("wishlist");
		System.out.println("Done setting up an in-memory database. Loading the data...");
		/* Reading the input */
		String line;
		
		BulkWriteOperation bulk = wishlistcollection.initializeUnorderedBulkOperation();
		int count = 0;
		int maxWid = -1;
		
		while ((line = br.readLine()) != null) {
			StringTokenizer token = new StringTokenizer(line);
			String personID = token.nextToken().replaceFirst("person", "");
			String wineID = token.nextToken().replaceFirst("wine", "");
			int pid = Integer.parseInt(personID);
			int wid = Integer.parseInt(wineID);
			
			if (wid < maxWid)
				wid = maxWid;
			
			if (count < 1000) {
				DBObject document = new BasicDBObject();
				document.put("personID", personID);
				document.put("wineID", wineID);
				bulk.insert(document);
				count++;
			}
			else { //if (count == 1000) {
				bulk.execute();
				count = 0;
				bulk = wishlistcollection.initializeUnorderedBulkOperation();
			}
		}
		System.out.println();
		br.close(); 
		System.out.println("Done reading the file. Creating indices for searching...");
		wishlistcollection.createIndex(new BasicDBObject("personID", 1));
		wishlistcollection.createIndex(new BasicDBObject("wineID", 1));		
		System.out.println("Done creating indices.....");
		
		DBCursor cursor = wishlistcollection.find();
		int bottleCount = 0;				// Total bottle count
		int currentPid = -1;				// Currently active Person ID
		int currentCount = 0;					// Number of bottles currently being sold
		try{
			File file =new File(resultFilename);
			if(!file.exists()){
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file,true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("                                                       \n"); // output the result
		
			BitSet wineTracker = new BitSet(maxWid + 1); // for tracking the unique wine bottles
			
			while(cursor.hasNext()) {
				int pid = Integer.parseInt((String) cursor.next().get("personID")); // friend id
				if (currentPid == pid && currentCount == 3)
					continue; // skip if the person already receives three bottles
			
				if (currentPid != pid) {
					currentPid = pid;
					currentCount = 0; // reset lastCnt
				}
				int wid = Integer.parseInt((String)cursor.next().get("wineID")); // bottle id
				if (wineTracker.get(wid)) 
					continue; // skip if the bottle is taken
				wineTracker.set(wid); // mark the bottle as "sold"
				bw.write(pid + "\t" + wid +"\n");
				currentCount++;
				bottleCount++;
			}
		bw.close();
		}catch(IOException ioe){
			System.out.println("Exception occurred:");
	    	ioe.printStackTrace();
		}
		// Write the count to the first line
		RandomAccessFile resultFile = new RandomAccessFile(new File(resultFilename), "rw");
		resultFile.write((bottleCount + "").getBytes());
		resultFile.close();	
		System.out.println("Number of bottles sold: " + bottleCount);		
	}
}
