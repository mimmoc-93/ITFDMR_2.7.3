package test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import utilities.Candidate;
import utilities.FDdiscovery;



public class InitialReduceTask {

	public static class InitialReducer
    extends Reducer<Text,MapWritable,Text,Text> {
 
	/*
	 * Map of candidate-entropy, level by level 1,2,n-1,n  
	 */
	
	Object2ObjectOpenHashMap<String, Double> candidateLevel1 = new Object2ObjectOpenHashMap<String,Double>();
	Object2ObjectOpenHashMap<String, Double> candidateLevel2 = new Object2ObjectOpenHashMap<String,Double>();
	Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1 = new Object2ObjectOpenHashMap<String,Double>();
	Object2ObjectOpenHashMap<String, Double> candidateLeveln = new Object2ObjectOpenHashMap<String,Double>();  
	int numAttribute;
	int recordNumber;
	//MultipleOutputs<Text, Text> mos;
	
	protected void setup(
				Reducer<Text, MapWritable, Text, Text>.Context context)
						throws IOException, InterruptedException {
		
		super.setup(context);
		numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));
		//mos = new MultipleOutputs(context);
		
		System.out.println("****************************************\n"
				         + "*** Starting Initial Reduce Task *******\n"
				         + "*** Attribute number : "+numAttribute+" ****\n"
				         		+ "****************************************");
		
	}
	  
	  
	public void reduce(Text key, Iterable<MapWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   
		
		/*
		 * Re-create candidate
		 */
		Candidate candidato = new Candidate(key.toString());  //ricreazione del candidato      
		int level_candidate = candidato.getLevel();
		
		/*
		 * POFV of candidate
		 */  
		for (MapWritable value : values) { 
 	  
			Iterator it = value.entrySet().iterator();
       
			while(it.hasNext()) { //H;CJ
				Map.Entry<Text,IntWritable> pair = (Entry<Text, IntWritable>) it.next();
			
				String chiave = pair.getKey().toString();
				int occorrenza = pair.getValue().get();
			
				if(candidato.get(chiave) == 0) {
					candidato.put(chiave, occorrenza);
				}else {
					int count = candidato.get(chiave);
					candidato.put(chiave, occorrenza+count);
				}
	        
			}
    
		}
   
   
		/*
		 * Calculate Entropy, approximation in 10 after comma
		 */

	
		if(recordNumber == 0) {
			recordNumber = candidato.getTotalRecord();
		}
		//System.out.println("Di volta in volta recodRumber = "+recordNumber+" \n\n");
		
		candidato.calculateEntropy(recordNumber);
		
		/*
		 * Adding candidate to respective map of level   
		 */
   
		if(level_candidate == 1)
			candidateLevel1.put(candidato.toString(), candidato.getEntropy());
		if(level_candidate == 2)
			candidateLevel2.put(candidato.toString(), candidato.getEntropy());
		if(level_candidate == (numAttribute-1)) 
			candidateLevelnminus1.put(candidato.toString(), candidato.getEntropy());
		if(level_candidate == numAttribute) 
			candidateLeveln.put(candidato.toString(), candidato.getEntropy());
   
		//System.out.println("**** Terminate reduce Function *************+ \n\n");//DEBUG
   
	}
 
	protected void cleanup(
			Reducer<Text,MapWritable,Text,Text>.Context context)
					throws IOException, InterruptedException {
 	
		System.out.println("\n\n********************************************\n"
				             + "**** Starting cleanup Initial Reducer *******\n"
				             + "**** Attribute Number: "+numAttribute+ " ********\n"
				             		+ "**** Record Number: "+recordNumber+" **********\n"
				             				+ "********************************************");
 	
 	
		/*
		 * Computing dependencies
		 * if recordnumber != 0 because maybe cleanup can be execute more than 1 time
		 */
		if(recordNumber != 0) {  
			
			ObjectArrayList<String> candidate_key = new ObjectArrayList<String>();
			ObjectArrayList<String> equivalent_key = new ObjectArrayList<String>();
			ObjectArrayList<String> FDs = new ObjectArrayList<String>();
			ObjectArrayList<String> nonDependants = new ObjectArrayList<>();
					
			
			/*
			 * copy level n and n-1 to find non dependants key, pruning rules 4
			 */
			
			Object2ObjectOpenHashMap<String, Double> oldLevelnminus1 = new Object2ObjectOpenHashMap<String,Double>();
			Iterator newit = candidateLevelnminus1.entrySet().iterator();
			while(newit.hasNext()) {
				Map.Entry<String, Double> pair = (Entry<String,Double>) newit.next();
				oldLevelnminus1.put(pair.getKey(), pair.getValue());
			}
 		
			Object2ObjectOpenHashMap<String, Double> oldLeveln = new Object2ObjectOpenHashMap<String,Double>();
			newit = candidateLeveln.entrySet().iterator();
			while(newit.hasNext()) {
				Map.Entry<String, Double> pair = (Entry<String,Double>) newit.next();
				oldLeveln.put(pair.getKey(), pair.getValue());
			}
 		
			
			
			/*
			 * search equivalent key and candidate key and write in temp File
			 */
			
			System.out.println("   Searching Equivalent and Candidate Keys...  ");
			FDdiscovery.searchKeyEquivalent(candidate_key,equivalent_key,recordNumber,candidateLevel1,candidateLevel2);
 		
			/*
			 * Write candidate and equivalent in configurationObject/file....
			 */
			String toAdd = "";
			for(int i=0; i<candidate_key.size();i++) {
				toAdd += candidate_key.get(i)+"|";
				System.out.println("\n - Candidate Key found: "+candidate_key.get(i)+"\n");
			}
			context.write(new Text("candidate-key"), new Text(toAdd));
			/*
			if(toAdd.length()>0) {
				mos.write(new Text("candidate-key"), new Text(toAdd.substring(0, toAdd.length()-1)), "configurationObject/");
			}else
				mos.write(new Text("candidate-key"), new Text(" "), "configurationObject/");
			*/
			
 		
			//System.out.println("\n******* Equivalent attribute found **********\n");
			toAdd = "";
			for(int i=0; i<equivalent_key.size();i++) {
				toAdd += equivalent_key.get(i)+"|";
				System.out.println("\n - Equivalent Key found: "+equivalent_key.get(i)+"\n");
			}
			context.write(new Text("equivalent-key"), new Text(toAdd));
			/*
			if(toAdd.length()>0) {
				//mos.write(new Text("equivalent-key"), new Text(toAdd.substring(0, toAdd.length()-1)), "configurationObject/");
			}else
				//mos.write(new Text("equivalent-key"), new Text(" "), "configurationObject/");
			*/
 		
			
			
			/*
			 * Perform pruning of superset of candidate keys and equivalent keys
			 */
 		
			System.out.println("\n    Performing pruning of Candidate Keys and Equivalent Keys ...... ");
			
			FDdiscovery.pruneCandidates(candidate_key,equivalent_key,candidateLevel1,candidateLevel2,
 				candidateLevelnminus1,candidateLeveln);
 	    
			System.out.println("\n    Pruning Completed!!! \n");
			
			
			/*
			 * Print new level
			 */
 		
			System.out.println("\n    Printing new Level after pruning...\n");
			
			System.out.println("------ New Level 1 --------\n");
			Iterator it = candidateLevel1.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println("   "+pair.getKey());
				
			}
		    System.out.println("-----------------------------\n\n");
 		
			System.out.println("--------- New Level 2 ----------\n");
			it = candidateLevel2.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println("   "+pair.getKey());
				
			}
			System.out.println("-----------------------------\n\n");
 		
			System.out.println("--------- New Level n-1 ----------\n");
			it = candidateLevelnminus1.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println("   "+pair.getKey());
				
			}
			System.out.println("-----------------------------\n\n");
 		
			System.out.println("--------- New Level n ----------\n");
			it = candidateLeveln.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println("   "+pair.getKey());
				
			}
			System.out.println("-----------------------------\n\n");
 		
			/*
			 * Check FDs using theorem 1
			 */
 		
			System.out.println("\n    Discovering FDs using theorem 1 ... \n");
			
			FDdiscovery.checkFDs(candidateLevel1,candidateLevel2,candidateLevelnminus1,candidateLeveln,FDs,nonDependants);
 		    
			//System.out.println("****** FD found *****\n\n");
			toAdd = "";
			for(int i=0; i<FDs.size(); i++) {
				toAdd += FDs.get(i)+"|";
				System.out.println("\n - FD found: " + FDs.get(i)+"\n");
			}
			context.write(new Text("FD"), new Text(toAdd));
			/*
			if(toAdd.length()>0) {
				//mos.write(new Text("FD"), new Text(toAdd.substring(0, toAdd.length()-1)), "configurationObject/");
				//System.out.println("write");
			}else {
				//mos.write(new Text("FD"), new Text(" "), "configurationObject/");
			}
			*/
			
			 		
			/*
			 * Find non-dependants candidate
			 */
			
			System.out.println("\n    Searching non-dependants candidate ... \n");
			
			
			FDdiscovery.checkNonDependants(oldLeveln, oldLevelnminus1, nonDependants);
 		
			toAdd = "";
			for(int i=0; i<nonDependants.size(); i++) {
				toAdd += nonDependants.get(i)+"|";
				System.out.println(" - Non dependants found: "+nonDependants.get(i));
			}
			context.write(new Text("non-dependants"), new Text(toAdd));
			
			System.out.println("\n    Non dependants candidate termined \n");
			/*
			if(toAdd.length()>0) {
				//mos.write(new Text("non-dependant"), new Text(toAdd.substring(0, toAdd.length()-1)), "configurationObject/");
			}else {
				//mos.write(new Text("non-dependant"), new Text(" "), "configurationObject/");
			}
			*/
			
			
			
		}
 	
		context.write(new Text("***"), new Text("***"));
		
		Iterator last = candidateLevel2.entrySet().iterator();
		while(last.hasNext()) {
 		
			Map.Entry<String, Double> pair = (Entry<String,Double>) last.next();
			//MapWritable tmp = new MapWritable();
			//Text key = new Text(pair.getKey());
			//DoubleWritable value = new DoubleWritable(pair.getValue());
			//tmp.put(key, value);
			//context.write(new Text(pair.getKey()),new DoubleWritable(pair.getValue()) );
			context.write(new Text(pair.getKey()),new Text(Double.toString(pair.getValue())) );
 		
		}
		
		System.out.println("\n*****************************************\n"
				+ "******* Terminating Initial Reduce *************\n"
				+ "************************************************\n\n\n");
		//mos.close();
	}
 
 
}
	
}
