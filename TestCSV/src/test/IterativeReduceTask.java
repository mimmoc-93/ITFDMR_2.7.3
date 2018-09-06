package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import utilities.Candidate;
import utilities.Connection;
import utilities.FDdiscovery;

public class IterativeReduceTask {

	public static class IterativeReducer
    extends Reducer<Text,MapWritable,Text,Text> {
		
		Object2ObjectOpenHashMap<String, Double> candidateLevelkminus1 = new Object2ObjectOpenHashMap<String,Double>();
		Object2ObjectOpenHashMap<String, Double> candidateLevelk = new Object2ObjectOpenHashMap<String,Double>();  
		Object2ObjectOpenHashMap<String, String> contextObject = null;

		int numAttribute;
		int recordNumber;
		int level;
		
		protected void setup(
				Reducer<Text, MapWritable, Text, Text>.Context context)
						throws IOException, InterruptedException {
			
			super.setup(context);
			numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));
			
			System.out.println("****************************************\n"
					         + "*** Starting Iterative Reduce Task *******\n"
					         + "*** Attribute number : "+numAttribute+" ****\n"
					         		+ "*******************************************");
			
			candidateLevelkminus1 = new Object2ObjectOpenHashMap<String, Double>();
			contextObject = new Object2ObjectOpenHashMap<String, String>();
			
	    	URI[] cacheFiles= context.getCacheFiles();
	        if(cacheFiles != null) {
	            for (URI cacheFile : cacheFiles) {
	                level = readFile(cacheFile,context,candidateLevelkminus1,contextObject);
	            }
	        }
			
	        if(candidateLevelkminus1.isEmpty()) {
	        	System.out.println("Empty level k.... terminating.....");
	        	System.exit(0);
	        }
	        
	        Iterator it = contextObject.entrySet().iterator();
	        while(it.hasNext()) {
	        	Map.Entry<String, String> pair = (Entry<String,String>) it.next();
	        	System.out.println("---------->  "+pair.getKey()+"   "+pair.getValue());
	        }
	        
			
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
			if(candidato.toString().equals("2,4,5")) {
				BigDecimal entr = candidato.calculateEntropyBig(recordNumber);
				
				System.out.println("Entropia di 2,4,5 :"+candidato.getEntropy()+"\n"+entr);
				
			}
			
			if(candidato.toString().equals("4,5,6")) {
				BigDecimal entr = candidato.calculateEntropyBig(recordNumber);
				
				System.out.println("Entropia di 4,5,6 :"+candidato.getEntropy()+"\n"+entr);
				
			}
			
			if(candidato.toString().equals("2,4,5,6")) {
				BigDecimal entr = candidato.calculateEntropyBig(recordNumber);
				
				System.out.println("Entropia di 2,4,5,6 :"+candidato.getEntropy()+"\n"+entr);
				
			}
			
			if(candidato.toString().equals("4,6,7")) {
				BigDecimal entr = candidato.calculateEntropyBig(recordNumber);
				
				System.out.println("Entropia di 4,6,7 :"+candidato.getEntropy()+"\n"+entr);
				
			}
			
			if(candidato.toString().equals("4,5,7")) {
				BigDecimal entr = candidato.calculateEntropyBig(recordNumber);
				
				System.out.println("Entropia di 4,5,7 :"+candidato.getEntropy()+"\n"+entr);
				
			}
			
			if(candidato.toString().equals("4,5,6,7")) {
				BigDecimal entr = candidato.calculateEntropyBig(recordNumber);
				
				System.out.println("Entropia di 4,5,6,7 :"+candidato.getEntropy()+"\n"+entr);
				
			}
			*/
			/*
			 * Adding candidate to respective map of level   
			 */
	   
			if(level_candidate == (level+1))
				candidateLevelk.put(candidato.toString(), candidato.getEntropy());
			
			//System.out.println("********** Terminating reduce Function in Iterative Reducer ***************");
		}
		
		protected void cleanup(
				Reducer<Text,MapWritable,Text,Text>.Context context)
						throws IOException, InterruptedException {
			
			System.out.println("\n\n********************************************\n"
		             + "*** Starting cleanup Iterative Reducer *******\n"
		             + "**** Attribute Number: "+numAttribute+ " ********\n"
		             		+ "Record Number: "+recordNumber+" **********\n\n");
			
			
			if(recordNumber != 0) {  
				
				ObjectArrayList<String> candidate_key = new ObjectArrayList<String>();
				ObjectArrayList<String> equivalent_key = new ObjectArrayList<String>();
				ObjectArrayList<String> FDs = new ObjectArrayList<String>();
				ObjectArrayList<String> nonDependants = new ObjectArrayList<String>();
				
				
				
				/*
				 * Parsing candidate key, equivalent key, FD, and nondepentants key in List of String
				 */
				FDdiscovery.parsingAtribute(candidate_key, equivalent_key, FDs, nonDependants, contextObject);
				
				System.out.println("FDs "+FDs.size());
				
				/*
				 * copy level n and n-1 to find non dependants key, pruning rules 4
				 */
				Object2ObjectOpenHashMap<String, Double> oldLevelkminus1 = new Object2ObjectOpenHashMap<String,Double>();
				Iterator newit = candidateLevelkminus1.entrySet().iterator();
				while(newit.hasNext()) {
					Map.Entry<String, Double> pair = (Entry<String,Double>) newit.next();
					oldLevelkminus1.put(pair.getKey(), pair.getValue());
				}
				
				Object2ObjectOpenHashMap<String, Double> oldLevelk = new Object2ObjectOpenHashMap<String,Double>();
				newit = candidateLevelk.entrySet().iterator();
				while(newit.hasNext()) {
					Map.Entry<String, Double> pair = (Entry<String,Double>) newit.next();
					oldLevelk.put(pair.getKey(), pair.getValue());
				}
				
				
				
				/*
				 * Before search candidate and equivalent prune candidate with attribute equivalent and candidate key
				 * from precedent level k-1
				 */
				
				System.out.println("*********** Pruning Candidate and equivalent key from precedent level k-1... ***************");
				for(int i=0; i<candidate_key.size(); i++) {
					
					FDdiscovery.removeCandidateKey(candidateLevelk, candidate_key.get(i)); 
					
				}
				
				for(int i=0; i<equivalent_key.size(); i++) {
					
					FDdiscovery.removeEquivalentKey(candidateLevelk, equivalent_key.get(i));
					FDdiscovery.removeEquivalentKey(candidateLevelkminus1, equivalent_key.get(i));

				}
				System.out.println("*********** Pruning Termined *************\n\n");
				
				
				/*
				 * Search candidate key in level k
				 */
				System.out.println("***********   Searching Candidate Key in level k  ***********\n");
				String toAdd = "";
				FDdiscovery.searchCandidateKey(candidate_key, recordNumber, candidateLevelk);
				for(int i=0; i<candidate_key.size(); i++) {
					toAdd += candidate_key.get(i)+"|";
					System.out.println("-------  "+candidate_key.get(i)+"  -----");
				}
				context.write(new Text("candidate-key"), new Text(toAdd));
				System.out.println("\n***********   Candidate Terminating ***********\n\n");
				
				
				
				/*
				 * Remove candidate key after searching in level k. Pruning
				 */
				System.out.println("\n***********  Removing Candidate key found ... ***********\n");
				
				for(int i=0; i<candidate_key.size(); i++) {
					
					FDdiscovery.removeCandidateKey(candidateLevelk, candidate_key.get(i));  
					
				}
				System.out.println("\n***********  Candidate key removing terminate ***********\n\n");
				
				
				
				/*
				 * Search equivalent key in k
				 */
				System.out.println("\n*********** Searching Equivalent key ... ***********\n");
				
				toAdd = "";
				FDdiscovery.searchEquivalentKey(equivalent_key, candidateLevelk, candidateLevelkminus1, level);
				
				for(int i=0; i<equivalent_key.size(); i++) {
					toAdd += equivalent_key.get(i)+"|";
					System.out.println("---- Equivalent key found:  "+equivalent_key.get(i)+"   \n");
				}
				context.write(new Text("equivalent-key"), new Text(toAdd));
				System.out.println("\n*********** Equivalent key Terminating ***********\n");
				
				
				
				/*
				 * Remove equivalent key. Pruning
				 */
				
				System.out.println("\n*********** Pruning Equivalent key ... ***********\n");
				//forse si dovrebbe fare solo per uno
				for(int i=0; i<equivalent_key.size(); i++) {
					
					FDdiscovery.removeEquivalentKey(candidateLevelk, equivalent_key.get(i));  
					FDdiscovery.removeEquivalentKey(candidateLevelkminus1, equivalent_key.get(i)); 
					
				}
				System.out.println("\n*********** Equivalent key pruning terminate ***********\n\n");
				
				
				System.out.println("\n\n New level k-1 ");
				Iterator it = candidateLevelkminus1.entrySet().iterator();
				while(it.hasNext()) {
					Map.Entry<String, Double> pair = (Entry<String,Double>) it.next();
					System.out.println("   "+pair.getKey());
				}
				System.out.println("\n ************** \n\n");
				
				System.out.println("\n\n New level k ");
				it = candidateLevelk.entrySet().iterator();
				while(it.hasNext()) {
					Map.Entry<String, Double> pair = (Entry<String,Double>) it.next();
					System.out.println("   "+pair.getKey());
				}
				System.out.println("\n ************** \n\n");
				
				
				if(candidateLevelk.size() == 0) {
					System.out.println("Empty Level..... Terminating......");
				}
				
				/*
				 * Calling now checkFDGeneric for pruning comparison with pruning rule 3 and 4
				 * and check FD non minimal
				 */
				
				ObjectArrayList<Connection> toCompute = new ObjectArrayList<Connection>();
				FDdiscovery.checkFdGeneric(toCompute, candidateLevelk, candidateLevelkminus1, FDs, nonDependants, true);
				
				/*
				 * Write in context FD found for next iteration
				 */
				toAdd = "";
				for(int i=0; i<FDs.size(); i++) {
					toAdd += FDs.get(i)+"|";
					System.out.println(" - FD found: " + FDs.get(i)+"\n");
				}
				context.write(new Text("FD"), new Text(toAdd));
				
				/*
				 * Write in context key non dependant found for next iteration
				 */
				toAdd = "";
				for(int i=0; i<nonDependants.size(); i++) {
					toAdd += nonDependants.get(i)+"|";
					System.out.println(" - Non dependants found: "+nonDependants.get(i));
				}
				context.write(new Text("non-dependants"), new Text(toAdd));
				
			}
			
			
			
			context.write(new Text("***"), new Text("***"));
			
			Iterator last = candidateLevelk.entrySet().iterator();
			while(last.hasNext()) {
	 		
				Map.Entry<String, Double> pair = (Entry<String,Double>) last.next();
				//MapWritable tmp = new MapWritable();
				//Text key = new Text(pair.getKey());
				//DoubleWritable value = new DoubleWritable(pair.getValue());
				//tmp.put(key, value);
				context.write(new Text(pair.getKey()),new Text(Double.toString(pair.getValue())) );
	 		
			}
			
		}
		
		private int readFile(URI fileURI,Context context,Object2ObjectOpenHashMap<String, Double> levelkminus1,
				Object2ObjectOpenHashMap<String, String> contextObject) {
			
			int numLevel = 0;
			
			context.getConfiguration().addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			context.getConfiguration().addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			
			Path pt=new Path(fileURI.toString());
			
			BufferedReader br = null;
			
			try {
				
				FileSystem fs = FileSystem.get(context.getConfiguration());
				br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				
				String line;
				line=br.readLine();
				
				boolean findCandidate = false;
				
				while (line != null){
					
					boolean toSwitch = false;
					
					if(line.startsWith("*"))
						toSwitch = true;
					
					if(findCandidate) {
						
						String[] lineSplit = line.split("\\s+");
			            
			            Candidate tmp = new Candidate(lineSplit[0]);
			            numLevel = tmp.getLevel();
			            
			            String candidate = lineSplit[0];
			            Double entropy = Double.parseDouble(lineSplit[1]);
			            levelkminus1.put(candidate, entropy);
						
					}else {
						if(!line.startsWith("*")) {
							
							String[] lineSplit = line.split("\\s+");
							
							if(lineSplit.length != 2)
								contextObject.put(line, "null");
							else
								contextObject.put(lineSplit[0], lineSplit[1]);
							
							
						}
					}
					
		            if(toSwitch)
		            	findCandidate = true;
		            
		            
		            line = br.readLine();
		        }
				
			}catch (Exception e){
				
				System.out.println(e.getMessage());
				System.exit(0);
				
			}finally {
				try {
					br.close();
				} catch (IOException e) {
					
					e.printStackTrace();
				}
			}
		
			return numLevel;
		}
		
	}
	
}
