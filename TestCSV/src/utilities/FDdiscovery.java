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
				System.out.println("Chiave trovata: "+key+" \n");
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
			Object2ObjectOpenHashMap<String, Double> candidateLevel2) {
		/*
		 * eliminate key
		 */
		if(candidate_key.size()>0) {
			for(int i=0; i<candidate_key.size();i++) {
				String key = candidate_key.get(i);
				if(candidateLevel1.get(key) != null)
					candidateLevel1.remove(key);
				
				if(candidateLevel2.get(key) != null)
					candidateLevel2.remove(key);
				
				
				Iterator it = candidateLevel2.entrySet().iterator();
				while(it.hasNext()) {
					
					Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
					String currentKey = pair.getKey();
					if(currentKey.contains(key))
						it.remove();
				}
			}
		}
		/*
		 * eliminate equivalent key
		 */
		if(equivalent_key.size()>0) {
			for(int i=0; i<equivalent_key.size();i++) {
				String key = equivalent_key.get(i);
				String[] equivalent = key.split(",");
				candidateLevel1.remove(equivalent[0]);
				
				Iterator it = candidateLevel2.entrySet().iterator();
				while(it.hasNext()) {
					
					Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
					String currentKey = pair.getKey();
					if(currentKey.contains(equivalent[0]))
						it.remove();
					
				}
			}
		}
		
	}
	

	public static void checkFDs(Object2ObjectOpenHashMap<String, Double> candidateLevel1,
			Object2ObjectOpenHashMap<String, Double> candidateLevel2,
			ObjectArrayList<String> FDs) {
		
		Iterator it = candidateLevel2.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String,Double>) it.next();
			String[] subkeys = pair.getKey().split(",");
			Double valuexy = pair.getValue();
			
			Double valuex = candidateLevel1.get(subkeys[0]);
			if(valuex != null) {
				if(valuexy.equals(valuex)) {
					System.out.println("Dipendenza trovata "+pair.getKey());
					FDs.add(subkeys[0]+" -> "+subkeys[1]);
				}
			}
			
			
		}
	
	}
	
}
