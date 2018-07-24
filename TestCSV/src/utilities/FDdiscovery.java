package utilities;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Reducer.Context;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class FDdiscovery {

	/*
	 * pruneCandidate()  Return in output
	 */
	public static void searchKeyEquivalent(ObjectArrayList<String> candidate_key, ObjectArrayList<String> equivalent_key
			,int numRecord,
			Object2ObjectOpenHashMap<String, Double> candidateLevel1,
			Object2ObjectOpenHashMap<String, Double> candidateLevel2) {
		
		//compute candidate keys
		Double key_distribution = 1/(double)numRecord;
		Double key_entropy = 0.0;
		key_entropy = Utility.log2(key_distribution);
		key_entropy = Utility.arrotonda(-key_entropy, 10);
		
		System.out.println("Entropy of key: "+key_entropy);
		
		/*
		 * pruning rules num.1 
		 * 
		 * Find key candidate from level1 to level2
		 *
		 */
		
		Iterator it = candidateLevel1.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String,Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
			//System.out.println("Confronto : "+key+" ; "+value+"  ->  "+key_entropy+"\n\n");
			if(value.equals(key_entropy)) {  //candidate key find
				//System.out.println("Chiave trovata: "+key+" \n");
				candidate_key.add(key);
			}
		}
		
		it = candidateLevel2.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String,Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
			//System.out.println("Confronto : "+key+" ; "+value+"  ->  "+key_entropy+"\n\n");
			if(value.equals(key_entropy)) {  //candidate key find
				//System.out.println("Chiave trovata: "+key+" \n");
				candidate_key.add(key);
			}
		}
				
		/*
		 * Serching equivalent key
		 */
		it = candidateLevel2.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double xy = pair.getValue();
			
			String[] key_sublevel = key.split(",");
			Double x = candidateLevel1.get(key_sublevel[0]);
			Double y = candidateLevel1.get(key_sublevel[1]);
			
			
			if( x.equals(y) && x.equals(xy)) {
				
				//System.out.println("Equivalenza trovata: "+key+" \n");
				equivalent_key.add(key);
				
			}
		}
		
		
	}
	
	public static void pruneCandidates(ObjectArrayList<String> candidate_key, 
			ObjectArrayList<String> equivalent_key,
			Object2ObjectOpenHashMap<String, Double> candidateLevel1,
			Object2ObjectOpenHashMap<String, Double> candidateLevel2,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1,
			Object2ObjectOpenHashMap<String, Double> candidateLeveln) {
		/*
		 * eliminate key
		 */
		if(candidate_key.size()>0) {
			for(int i=0; i<candidate_key.size();i++) {
				String key = candidate_key.get(i);
				
				/*
				 * Remove candidate key from level 1, 2, n-1, n
				 */
				if(candidateLevel1.get(key) != null)
					candidateLevel1.remove(key);
				
				removeCandidateKey(candidateLevel2, key);
				removeCandidateKey(candidateLevelnminus1, key);
				removeCandidateKey(candidateLeveln, key);
				
				/*
				 * End of pruning candidate keys
				 */
				
			}
		}
		/*
		 * eliminate equivalent key
		 */
		if(equivalent_key.size()>0) {
			for(int i=0; i<equivalent_key.size();i++) {
				String key = equivalent_key.get(i);
				String[] equivalent = key.split(",");
				/*
				 * Remove equivalent key from level 1
				 */
				candidateLevel1.remove(equivalent[0]);
				
				/*
				 * Remove equivalent key from level 2, n-1 ,n 
				 */
				
				removeEquivalentKey(candidateLevel2, equivalent[0]);
				removeEquivalentKey(candidateLevelnminus1, equivalent[0]);
				removeEquivalentKey(candidateLeveln, equivalent[0]);
				

				
			}
		}
		
	}
	
	public static void removeEquivalentKey(Object2ObjectOpenHashMap<String, Double> candidateLevel, String key) {
		Iterator it = candidateLevel.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
			String currentKey = pair.getKey();
			if(currentKey.contains(key))
				it.remove();
		}
		
	}
	
	public static void removeCandidateKey(Object2ObjectOpenHashMap<String, Double> candidateLevel, String key) {
		
		Iterator it = candidateLevel.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
			String currentKey = pair.getKey();
			String[] key_single = key.split(",");
			int count=0;
			
			for(int j=0; j<key_single.length;j++) {
				if(currentKey.contains(key_single[j]))
					count++;
			}
			if(count == key_single.length) {
				System.out.println("rimuovi "+currentKey+"  "+key);
				it.remove();
			}
		}
		
	}

	public static void checkFDs(Object2ObjectOpenHashMap<String, Double> candidateLevel1,
			Object2ObjectOpenHashMap<String, Double> candidateLevel2,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1,
			Object2ObjectOpenHashMap<String, Double> candidateLeveln,
			ObjectArrayList<String> FDs) {
		
		/*
		 * Just for level 1,2
		 * TODO: level n and n-1
		 */
		
		Iterator it = candidateLevel2.entrySet().iterator();  //Start from the bottom... so level 2 to 1 to discovery in level1
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String,Double>) it.next();
			String[] keyxy = pair.getKey().split(",");
			Double valuexy = pair.getValue();
			
			Double valuex = candidateLevel1.get(keyxy[0]);
			Double valuey = candidateLevel1.get(keyxy[1]);
			
			if(valuex != null) {
				if(valuexy.equals(valuex)) {
					System.out.println("Dipendenza trovata "+pair.getKey()+" "+keyxy[0]);
					FDs.add(keyxy[0]+" -> "+keyxy[1]);
				}
			}
			
			if(valuey != null) {
				if(valuexy.equals(valuey)) {
					System.out.println("Dipendenza trovata "+pair.getKey()+" "+keyxy[1]);
					FDs.add(keyxy[1]+" -> "+keyxy[0]);
				}
			}
			
			
		}
		/*
		 * Now for level n using n-1. 
		 * This function is general from check FD to level k and k-1
		 * 
		 */
		
		ObjectArrayList<Connection> toCompute = new ObjectArrayList<Connection>();
		checkFdGeneric(toCompute, candidateLeveln, candidateLevelnminus1, FDs);
		
		System.out.println("DEBUG");
	}
	
	public static void checkFdGeneric(ObjectArrayList<Connection> toCompute,
			Object2ObjectOpenHashMap<String, Double> candidateLeveln,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1,
			ObjectArrayList<String> FDs) {
		
		generateEdge(toCompute,candidateLeveln,candidateLevelnminus1);
		
		for(int i=0; i<toCompute.size(); i++) {
			if(toCompute.get(i).check()) {
				System.out.println("FD trovata");
				FDs.add(toCompute.get(i).getFD());
			}
		}
		
	}
	
	public static void generateEdge(ObjectArrayList<Connection> toCompute, 
			Object2ObjectOpenHashMap<String, Double> candidateLeveln,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1)	{

		Iterator it = candidateLeveln.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String,Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
			
			Iterator it2 = candidateLevelnminus1.entrySet().iterator();
			while(it2.hasNext()) {
				
				Map.Entry<String, Double> newpair = (Entry<String, Double>) it2.next();
				String[] subkey = newpair.getKey().split(",");
				Double newvalue = newpair.getValue();
				
				if(isTrue(key,subkey)) {
					Connection tmp = new Connection(key,newpair.getKey(),value,newvalue);
					toCompute.add(tmp);
				}
				
			}
		}
		
		
	}
	
	public static boolean isTrue(String n, String[] check) {
		
		int count =0;
		for(int i=0; i<check.length; i++) {
			if(n.contains(check[i]))
				count++;
		}
		
		if(count == check.length)
			return true;
		else
			return false;	
		
	}
	
	public static void checkNonDependants(Object2ObjectOpenHashMap<String, Double> candidateLeveln,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1,
			ObjectArrayList<String> nonDependants ) {
		
		Iterator itn = candidateLeveln.entrySet().iterator();
		while(itn.hasNext()) {
			Map.Entry<String, Double> pairn = (Entry<String,Double>) itn.next();
			String keyn = pairn.getKey();
			Double valn = pairn.getValue();
			
			Iterator it = candidateLevelnminus1.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String,Double>) it.next();
				String key = pair.getKey();
				Double value = pair.getValue();
				
				if(!value.equals(valn)) {
					String[] keyn_values = keyn.split(",");
					System.out.println("NON UGUALE"+key);
					
					for(int i=0; i<keyn_values.length; i++) {
						if(!key.contains(keyn_values[i])) {
							nonDependants.add(keyn_values[i]);
							
						}
					}
					
				}
					
				
			}
		}
		
	}
	
}
