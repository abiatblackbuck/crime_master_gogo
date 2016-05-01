package crime.master.gogo.crime.migrategeodata;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Transformer implements Runnable {
	private List<BasicDBObject> dbo; 
	public Transformer(List<BasicDBObject> dbo) {
		this.dbo = dbo;
	}

	@Override
	public void run() {
		try {
			CopyOnWriteArrayList<DBObject> docList = new CopyOnWriteArrayList<>();
			DB db = null;
			try {
				db = MigrateGeoData.getMongoClient().getDB("tracking");				
			} catch (Exception ex) {
				System.out.println("************DB not found************");
			}	
			DBCollection coll = db.getCollection("geoJSONData");
			for(BasicDBObject eachObj: dbo) {
				Set<String> keySet = eachObj.keySet();
				Double latitude = null;
				Double longitude = null;
				BasicDBObject newDBo = new BasicDBObject();
	
				for(String str : keySet) {
					if(str.equals("_id")) {
						continue;
					}
					if(str.equals("latitude")) {
						try{
							latitude = Double.parseDouble((eachObj.getString(str)));
						}catch(Exception ex) {
							latitude = 0d;
						}
						continue;
					}
					if(str.equals("longitude")) {
						try {
							longitude = Double.parseDouble((eachObj.getString(str)));
						}catch(Exception ex) {
							longitude = 0d;
						}
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
			
			coll.insert(docList);
		} catch (Exception ex) {
			System.out.println("Issue: " + ex.getMessage() + " Type: " + ex.getClass() + " Cause: " + ex.getCause());
		}
	}
}
