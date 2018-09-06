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
	
	/*
	 * Search candidate Key
	 */
	
	public static void searchCandidateKey(ObjectArrayList<String> candidate_key, int numRecord,
			Object2ObjectOpenHashMap<String, Double> candidateLevelk) {
		
		//compute candidate keys
		Double key_distribution = 1/(double)numRecord;
		Double key_entropy = 0.0;
		key_entropy = Utility.log2(key_distribution);
		key_entropy = Utility.arrotonda(-key_entropy, 10);
		System.out.println("\n Candidate key: "+key_entropy+"\n");
		
		Iterator it = candidateLevelk.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String,Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
			//System.out.println("Confronto : "+key+" ; "+value+"  ->  "+key_entropy+"\n\n");
			if(value.equals(key_entropy)) {  //candidate key find
				//System.out.println("Chiave trovata: "+key+" \n");
				candidate_key.add(key);
				System.out.println("Candidate key founf: keyEntropy "+key_entropy+"  candidate entropy "+value+ " - "+key);
			}
		}
		
	}
	
	/*
	 * Search equivalent key
	 */
	
	public static void searchEquivalentKey(ObjectArrayList<String> equivalent_key, 
			Object2ObjectOpenHashMap<String, Double> candidateLevelk,
			Object2ObjectOpenHashMap<String, Double> candidateLevelkminus1,
			int level) {
		
		Iterator it = candidateLevelk.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
			
			/*
			 * Generating the components of level k-1 from the candidate
			 * Similiar at generatingEdge
			 */
			
			
			Object2ObjectOpenHashMap<String, Double> component = new Object2ObjectOpenHashMap<String,Double>();
			String[] componentSupport = new String[level+1];
			int count=0;
			Iterator it2 = candidateLevelkminus1.entrySet().iterator();
			while(it2.hasNext() && count<(level+1)) {
				
				Map.Entry<String, Double> pair2 = (Entry<String,Double>) it2.next();
				String[] subkey = pair2.getKey().split(",");
				Double newvalue = pair2.getValue();
				
				if(isTrue(key,subkey)) {
					componentSupport[count] = pair2.getKey();
					component.put(pair2.getKey(), newvalue);
					count++;
				}
				
			}
			/*
			 * Search equivalent key
			 */
			
			System.out.print("\nCandidate "+key+" "+value+"  Subcandidate \n");
			for(int i=0; i<componentSupport.length; i++) {
				System.out.print(componentSupport[i]+" "+component.get(componentSupport[i])+" ");
			}
			System.out.print("\n");
			int count_equivalent = 0;
			
			for(int i=0; i<componentSupport.length; i++) {
				if(componentSupport[i] != null) {
					if(component.get(componentSupport[i]).equals(value)) {
						count_equivalent++;
					}else
						componentSupport[i] = null;
				}
				
			}
			
			int toEliminate = count_equivalent-1;
			
			//System.out.println(" For Candidate "+key+ " ----->  toEliminate = "+toEliminate+" \n");
			
			if(count_equivalent > 1) {
				for(int i=0; i<componentSupport.length; i++) {
					if(componentSupport[i] != null && toEliminate > 0) {
						//System.out.println("Eliminating "+componentSupport[i]);
						equivalent_key.add(componentSupport[i]);
						toEliminate--;
					}
				}
			}
			System.out.println("  ------------------------------------------------------ ");
		}
		
		
	}
	
	
	public static void searchEquivalentKey2(ObjectArrayList<String> equivalent_key, 
			Object2ObjectOpenHashMap<String, Double> candidateLevelk,
			Object2ObjectOpenHashMap<String, Double> candidateLevelkminus1,
			int level) {
		
		Iterator it = candidateLevelk.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
		
			Iterator it2 = candidateLevelkminus1.entrySet().iterator();
			
			boolean finded = false;
			int find = 0;
			
			while(it2.hasNext() && finded) {
				
				Map.Entry<String, Double> pair2 = (Entry<String,Double>) it2.next();
				String[] subkey = pair2.getKey().split(",");
				Double newvalue = pair2.getValue();
				
				if(isTrue(key,subkey)) {
					find++;
				}
				
			}
			
			
		}
		
		
	}
	
	
	/*
	 * Prune Candidate
	 */
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
			String[] key_single = key.split(",");
			int count=0;
			
			for(int j=0; j<key_single.length;j++) {
				if(currentKey.contains(key_single[j]))
					count++;
			}
			if(count == key_single.length) {
				System.out.println("rimuovi equivalent key "+currentKey+"  "+key);
				it.remove();
			}
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
				System.out.println("rimuovi candidate key "+currentKey+"  "+key);
				it.remove();
			}
		}
		
	}

	public static void checkFDs(Object2ObjectOpenHashMap<String, Double> candidateLevel1,
			Object2ObjectOpenHashMap<String, Double> candidateLevel2,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1,
			Object2ObjectOpenHashMap<String, Double> candidateLeveln,
			ObjectArrayList<String> FDs,
			ObjectArrayList<String> nonDependants) {
		
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
					FDs.add(keyxy[0]+"->"+keyxy[1]);
				}
			}
			
			if(valuey != null) {
				if(valuexy.equals(valuey)) {
					System.out.println("Dipendenza trovata "+pair.getKey()+" "+keyxy[1]);
					FDs.add(keyxy[1]+"->"+keyxy[0]);
				}
			}
			
			
		}
		/*
		 * Now for level n using n-1. 
		 * This function is general from check FD to level k and k-1
		 * 
		 */
		
		ObjectArrayList<Connection> toCompute = new ObjectArrayList<Connection>();
		checkFdGeneric(toCompute, candidateLeveln, candidateLevelnminus1, FDs, nonDependants, false);
		
		System.out.println("DEBUG");
	}
	
	public static void checkFdGeneric(ObjectArrayList<Connection> toCompute,
			Object2ObjectOpenHashMap<String, Double> candidateLeveln,
			Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1,
			ObjectArrayList<String> FDs,
			ObjectArrayList<String> nonDependants,
			boolean pruning) {
		
		generateEdge(toCompute,candidateLeveln,candidateLevelnminus1);
		
		if(pruning) {
			pruningComparison(toCompute, FDs, nonDependants); //Pruning Non Dependants and discover only minimal FD
		}
		
		for(int i=0; i<toCompute.size(); i++) {
			if(toCompute.get(i).check()) {
				System.out.println("FD trovata  -> "+toCompute.get(i).getFD2());
				FDs.add(toCompute.get(i).getFD2());
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
	
	public static void pruningComparison(ObjectArrayList<Connection> toCompute, ObjectArrayList<String> FDs,
										 ObjectArrayList<String> nonDependants) {
		
		System.out.println("******* Pruning Comparison from non dependant key and discover only Minimal FD *********************");
		
		for(int i=0; i<toCompute.size(); i++) {
			System.out.println(toCompute.get(i).toString());
			/* Attribute non dependants
			 * Remove comparison when non dependant attribute is in RHS side
			 * but not remove when non dependant attribute is in LHS side
			 * 
			 * LHS is candidate with minor size ex 0,1
			 * RHS is candidate with max size ex 0,1,2
			 */
			
			String LHS = toCompute.get(i).cand2;
			String RHS = toCompute.get(i).cand1;
			
			
			
			boolean removed = false;
			
			for(int j=0; j<nonDependants.size(); j++) {
				if(RHS.contains(nonDependants.get(j))) {
					if(!LHS.contains(nonDependants.get(j))) {
						System.out.println(" - Removing non dependants: "+LHS+" --- "+RHS+"\n");
						toCompute.remove(i);
						removed = true;
						break;
					}
				}
			}
			
			
			/*
			 * Discover Only Minimal FD
			 * if H(x) = H(XY) => H(XA) = H(XYA) 
			 * remove comparison with A
			 */
			
			if(!removed) {
				
				for(int j=0; j<FDs.size(); j++) {
					
					if(Utility.minimalFD(LHS, RHS, FDs.get(j))){
						System.out.println(" - Removing nonMinimal: "+LHS+" --- "+RHS+"\n");
						toCompute.remove(i);
						break;
					}
				}	
			}
		}

		System.out.println("******* Pruning Terminated ************");
	}
	
	public static void parsingAtribute(ObjectArrayList<String> candidate_key,
			ObjectArrayList<String> equivalent_key,
			ObjectArrayList<String> FDs,
			ObjectArrayList<String> nonDependants,
			Object2ObjectOpenHashMap<String, String> contextObject) {
		
		if(contextObject.get("candidate-key")!=null) {
			String[] candidate = contextObject.get("candidate-key").split("\\|");
			for(int i=0; i<candidate.length; i++) {
				candidate_key.add(candidate[i]);
			}
		}
		
		if(contextObject.get("equivalent-key") != null) {
			String[] equivalent = contextObject.get("equivalent-key").split("\\|");
			for(int i=0; i<equivalent.length; i++) {
				equivalent_key.add(equivalent[i]);
			}
		}
		
		if(contextObject.get("FD")!=null) {
			String[] FD = contextObject.get("FD").split("\\|");
			for(int i=0; i<FD.length; i++) {
				FDs.add(FD[i]);
			}
		}
		
		if(contextObject.get("non-dependants") != null) {
			String[] nonDep = contextObject.get("non-dependants").split("\\|");
			for(int i=0; i<nonDep.length; i++) {
				nonDependants.add(nonDep[i]);
			}
		}	
		
		
		
	}
	
}
