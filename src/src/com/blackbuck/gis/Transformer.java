package com.blackbuck.gis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Transformer implements Runnable {
	private List<DBObject> dbo; 
	public Transformer(List<DBObject> dbo) {
		this.dbo = dbo;
	}

	@Override
	public void run() {
		List<DBObject> docList = new ArrayList<>();
		DB db = null;
		for(DBObject eachObj: dbo) {
			Set<String> keySet = eachObj.keySet();
			Double latitude = null;
			Double longitude = null;
			BasicDBObject newDBo = new BasicDBObject();
			try {
				db = MigrateGeoData.getMongoClient().getDB("tracking");
			} catch (Exception ex) {
				
			}
			for(String str : keySet) {
				if(str.equals("latitude")) {
					latitude = (Double)eachObj.get(str);
					continue;
				}
				if(str.equals("longitude")) {
					longitude = (Double)eachObj.get(str);
					continue;
				}
				newDBo.put(str, eachObj.get(str));
			}	
			BasicDBObject loc = new BasicDBObject();
			loc.put("type", "Point");
			List<Double> coordinates = new ArrayList<>();
			coordinates.add(longitude);
			coordinates.add(latitude);
			loc.put("coordinates", coordinates);
			newDBo.put("loc", loc);
			docList.add(newDBo);
		}
		DBCollection coll = db.getCollection("newGeoLocation");
		coll.insert(docList);
	}
}
