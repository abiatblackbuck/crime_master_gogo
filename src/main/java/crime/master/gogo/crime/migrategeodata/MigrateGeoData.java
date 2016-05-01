package crime.master.gogo.crime.migrategeodata;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

import crime.master.gogo.crime.CrimeMaster;


public class MigrateGeoData implements CrimeMaster {
	private Integer NUM_THREADS;
	public static final Integer BATCH_SIZE =1000;
	private static Mongo client;	
	
	public static Mongo getMongoClient() throws Exception {
		if(null == client){
			synchronized (MigrateGeoData.class){
				if(null == client) {
					try {
						client = new Mongo();
					} catch(Exception ex) {
						client = null;
					}
				}
			}
		}
		return client;
	}	

	public MigrateGeoData(Integer nUM_THREADS) {
		super();
		NUM_THREADS = nUM_THREADS;
	}

	public Integer getNUM_THREADS() {
		return NUM_THREADS;
	}

	public void setNUM_THREADS(Integer nUM_THREADS) {
		NUM_THREADS = nUM_THREADS;
	}

	public void doTheCrime() throws Exception {	
		DB db = getMongoClient().getDB("tracking");
		DBCollection coll = db.getCollection("truckLocation");	
		Long countDocs = coll.count();
		int batchCount = 50;
		int limit = batchCount*BATCH_SIZE;	
		int iters = (int) (countDocs/limit);
		int skip = 0;

		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);			
		for(int i=0; i<iters; i++) {
			int bc = 0;
			System.out.println("Submitted new cursor" + (i+1));
			skip = i * limit;
			DBCursor cursor = coll.find().skip(skip).limit(limit);
			while(bc < batchCount  && cursor.hasNext()) {
				++bc;
				int index = BATCH_SIZE;
				CopyOnWriteArrayList<BasicDBObject> docList = new CopyOnWriteArrayList<>();	
				while(index > 0 && cursor.hasNext()) {
					BasicDBObject dbo = (BasicDBObject)cursor.next();
					docList.add(dbo);
					index--;
				}	
				Runnable worker = new Transformer(docList);
				executor.execute(worker);
			}
			Thread.sleep(5000);
		}
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");	
	}
}
