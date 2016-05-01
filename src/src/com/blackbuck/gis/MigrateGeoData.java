package com.blackbuck.gis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


public class MigrateGeoData {
	public static final Integer NUM_THREADS = 100;
	public static final Integer BATCH_SIZE = 100;
	
	public static Mongo getMongoClient() throws Exception{
		Mongo  client = new Mongo();
		return client;
	}
	
	public static void main(String[] args) throws Exception {		
		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);		
		DB db = getMongoClient().getDB("tracking");
		DBCollection coll = db.getCollection("truckLocation");
		DBCursor cursor = coll.find();		
		List<DBObject> docList = new ArrayList<>();
		boolean once = true;
		while(once && cursor.hasNext()) {
			int index = BATCH_SIZE;
			while(index > 0 && cursor.hasNext()) {
				DBObject dbo = cursor.next();
				docList.add(dbo);
				index--;
			}						
			Runnable worker = new Transformer(docList);
			executor.execute(worker);
			once = false;
		}
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");					
	}

}
