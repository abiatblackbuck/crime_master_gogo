package crime.master.gogo;

import crime.master.gogo.crime.CrimeFactory;
import crime.master.gogo.crime.CrimeMaster;

public class ExecuteCrime {
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Correct usage is <CRIME> <NUM_THREADS>");
		}
		
		int num_threads = Integer.parseInt(args[1]);
		String crimeData = args[0];
		CrimeMaster crime = CrimeFactory.getCrime(crimeData, num_threads);
		crime.doTheCrime();
	}

}
