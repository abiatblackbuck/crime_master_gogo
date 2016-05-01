package crime.master.gogo.crime;

import crime.master.gogo.crime.migrategeodata.MigrateGeoData;

public class CrimeFactory {
	
	public static CrimeMaster getCrime(String crime, Integer num_threads) {
		if(crime.equals("migrateGeoData")) {
			return new MigrateGeoData(num_threads);
		} else {
			return null;
		}
	}

}
