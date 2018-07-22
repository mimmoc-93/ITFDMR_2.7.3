package test;
import utilities.*;

import it.unimi.dsi.fastutil.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import JavaMI.Entropy;


public class TestCsv {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable>{

    MapWritable HM_ToReturn = null;
    Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>> maplevel = null;
    
    
    @Override
    protected void setup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    	System.out.println("\n\n------------------Setup del map------------------\n\n");
    	//I candidati non possono essere creati perchè non si è a conoscenza del numero di attributi
    	
    	
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
      StringTokenizer itr = new StringTokenizer(value.toString()); //record in input
      
      while (itr.hasMoreTokens()) { 
        String record = itr.nextToken();
        String[] recordSplit = record.split(",");
        
        int numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));
        if(numAttribute == 0) {
      	  int newNum = recordSplit.length;
      	  context.getConfiguration().set("numAttribute", String.valueOf(newNum));
        }
        
        
        /*
    	 * Creation of the level. 
    	 * If level are not created yet create it
    	*/
    	if(maplevel == null) {  
    		
    		maplevel = Utility.generateCandidateList(record);
    		
    		System.out.println("**** Generazione dei livelli **** \n\n");
    		
    		
    		System.out.println("Livello 1 \n\n");
            ObjectArrayList<Candidate> tmp = maplevel.get("1");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            /*
            System.out.println("\n\nLivello 2 \n\n");
            tmp = maplevel.get("2");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\nLivello n \n\n");
            tmp = maplevel.get("n");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\nLivello n-1 \n\n");
            tmp = maplevel.get("n-1");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\n");
    		*/
            System.out.println("\n\n***************\n\n");
    	}
        
    	
        
        
    	
    	/*
    	 *  Partial PMF for every candidate, level by level 1,2,n,n-1
    	 */
    	
    	String[] level_to_compute = {"1","2","n-1","n"};
    	//String[] level_to_compute = {"1"};
    	ObjectArrayList<Candidate> currentLevel = null;
    	
    	for(int i=0; i<level_to_compute.length; i++) {
    		
    		currentLevel = maplevel.get(level_to_compute[i]);
    		for(int j=0; j<currentLevel.size(); j++) {
        		
        		String tupla = currentLevel.get(j).getTupla(recordSplit);
        				
        		if(currentLevel.get(j).get(tupla) == 0 ){
        				
        			currentLevel.get(j).put(tupla, 1);
        				
        		}else {
        				
        			int tmp = currentLevel.get(j).get(tupla);
        			currentLevel.get(j).put(tupla, tmp+1);
        				
        		}
           	}
    	}
    	/*
    	 * Finish PMF
    	 */
    	
      }
    }
    
    @Override
    protected void cleanup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    	
    	for(ObjectArrayList<Candidate> level: maplevel.values()) {
    		//System.out.println("**************Livello "+currentLevel+"    *******************************");
    		
    		for(int i=0; i<level.size(); i++) { //level
    			HM_ToReturn = new MapWritable();
    			//candidate
    			Candidate tmp = level.get(i);
    			Text candidate = new Text(tmp.toString());
    			//System.out.println("------- Candidato = "+candidate+"    ---------------");
    			Iterator it = tmp.getIterator();
    			while(it.hasNext()) {
    				Map.Entry<String, Integer> pair = (Entry<String,Integer>) it.next();
    				Text chiave = new Text(pair.getKey());
    				IntWritable valore = new IntWritable(pair.getValue());
    				
    				//System.out.println(chiave+" -> "+valore);
    				HM_ToReturn.put(chiave, valore);
    			}
    			context.write(candidate, HM_ToReturn);
    		}
    		
    		//System.out.println("*********************************************************************");
    		
    	}
    	
    	
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,MapWritable,Text,DoubleWritable> {
    
	/*
	 * Map of candidate-entropy, level by level 1,2,n-1,n  
	 */
	
	Object2ObjectOpenHashMap<String, Double> candidateLevel1 = new Object2ObjectOpenHashMap<String,Double>();
	Object2ObjectOpenHashMap<String, Double> candidateLevel2 = new Object2ObjectOpenHashMap<String,Double>();
	Object2ObjectOpenHashMap<String, Double> candidateLevelnminus1 = new Object2ObjectOpenHashMap<String,Double>();
	Object2ObjectOpenHashMap<String, Double> candidateLeveln = new Object2ObjectOpenHashMap<String,Double>();  
	
	int numAttribute;
	
    protected void setup(
				Reducer<Text, MapWritable, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {
    	
    	super.setup(context);
    	numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));
	
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
  
      int recordNumber  = Integer.parseInt(context.getConfiguration().get("recordNumber"));
      if(recordNumber == 0) {
    	  int newRecord = candidato.getTotalRecord();
    	  context.getConfiguration().set("recordNumber", String.valueOf(newRecord));
      }
      int newRecord = Integer.parseInt(context.getConfiguration().get("recordNumber"));
      
      candidato.calculateEntropy(newRecord);
     
      
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
      
      //System.out.println("---------------------------------------------------------------------------------------- \n\n");//DEBUG
      
    }
    
    protected void cleanup(
			Reducer<Text,MapWritable,Text,DoubleWritable>.Context context)
					throws IOException, InterruptedException {
    	
		/*
		 * Retriving from Context Object number of attribute and number of record
		 */
    	int numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));
    	int recordNumber = Integer.parseInt(context.getConfiguration().get("recordNumber"));
    	System.out.println("********************* Il numero degli attributi è :"+numAttribute+" ****************\n\n");
    	
    	
    	
    	/*
    	 * Computing dependencies
    	 * if recordnumber != 0 because maybe cleanup can be execute more than 1 time
    	 */
    	if(recordNumber != 0) {  
    		ObjectArrayList<String> candidate_key = new ObjectArrayList<String>();
    		ObjectArrayList<String> equivalent_key = new ObjectArrayList<String>();
    		ObjectArrayList<String> FDs = new ObjectArrayList<String>();
    		
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
    		 * search equivalent key
    		 */
    		FDdiscovery.searchKeyEquivalent(candidate_key,equivalent_key,recordNumber,candidateLevel1,candidateLevel2);
    		
    		Configuration conf = context.getConfiguration();
    		String toAdd = conf.get("candidate-Key");
    		
    		System.out.println("**** Candidate key found ****\n");
    		for(int i=0; i<candidate_key.size();i++) {
    			//scrivere le occorrenze trovate nel contesto //
    			toAdd += "|"+candidate_key.get(i);
    		}
    		conf.set("candidate-Key", toAdd);
    		System.out.println("\n*****************************\n");
    		
    		
    		
    		System.out.println("\n******* Equivalent attribute found **********\n");
    		toAdd = conf.get("equivalent-Key");
    		for(int i=0; i<equivalent_key.size();i++) {
    			//scrivere le occorrenze trovate nel contesto //
    			toAdd += "|"+equivalent_key.get(i);
    			//System.out.println(equivalent_key.get(i));
    		}
    		conf.set("equivalent-Key", toAdd);
    		System.out.println("\n*********************************************\n");
    		
    		/*
    		 * Perform pruning of superset of candidate keys and equivalent keys
    		 */
    		
    		FDdiscovery.pruneCandidates(candidate_key,equivalent_key,candidateLevel1,candidateLevel2,
    				candidateLevelnminus1,candidateLeveln);
    	    
    		/*
    		 * Print new level
    		 */
    		
    		System.out.println("------ New Level 1 --------\n");
    		Iterator it = candidateLevel1.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println(pair.getKey());
				
			}
    		System.out.println("-----------\n\n");
    		
    		System.out.println("--------- New Level 2 ----------\n");
    		it = candidateLevel2.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println(pair.getKey());
				
			}
    		System.out.println("-----------\n\n");
    		
    		System.out.println("--------- New Level n-1 ----------\n");
    		it = candidateLevelnminus1.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println(pair.getKey());
				
			}
    		System.out.println("-----------\n\n");
    		
    		System.out.println("--------- New Level n ----------\n");
    		it = candidateLeveln.entrySet().iterator();
			while(it.hasNext()) {
				
				Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
				System.out.println(pair.getKey());
				
			}
    		System.out.println("-----------\n\n");
    		
    		/*
    		 * Check FDs using theorem 1
    		 */
    		
    		FDdiscovery.checkFDs(candidateLevel1,candidateLevel2,candidateLevelnminus1,candidateLeveln,FDs);
    		
    		System.out.println("****** FD found *****\n\n");
    		toAdd = conf.get("FD");
    		for(int i=0; i<FDs.size(); i++) {
    			//save to Configuration Object
    			toAdd += "|"+FDs.get(i);
        		System.out.println(FDs.get(i));
        	}
    		conf.set("FD", toAdd);
    		System.out.println("\n\n**********************");
    		
    		/*
    		 * Find non-dependants candidate
    		 */
    		
    		ObjectArrayList<String> nonDependants = new ObjectArrayList<>();
    		FDdiscovery.checkNonDependants(oldLeveln, oldLevelnminus1, nonDependants);
    		
    		toAdd = conf.get("dependant-Key");
    		System.out.println("******** Non Dependants ******\n\n");
    		for(int i=0; i<nonDependants.size(); i++) {
    			//save to configuration Object
    			toAdd += "|"+nonDependants.get(i);
    			System.out.println(nonDependants.get(i));
    		}
    		conf.set("dependant-Key", toAdd);
    		System.out.println("\n\n*************************************");
    	}
    	
    	Iterator it_conf = context.getConfiguration().iterator();
    	while(it_conf.hasNext()) {
    		Map.Entry<String, String> pair = (Entry<String, String>) it_conf.next();
    		String key = pair.getKey();
    		String value = pair.getValue();
    		if(key.equals("candidate-Key"))
    			System.out.println("Candidate-Key"+value+"\n\n");
    		if(key.equals("equivalent-Key"))
    			System.out.println("Equivalent-Key"+value+"\n\n");
    		if(key.equals("FD"))
    			System.out.println("FD"+value+"\n\n");
    		if(key.equals("dependant-Key"))
    			System.out.println("Dependand-Key"+value+"\n\n");
    	}
    	
    	
    	Iterator last = candidateLevel2.entrySet().iterator();
    	while(last.hasNext()) {
    		
    		Map.Entry<String, Double> pair = (Entry<String,Double>) last.next();
    		context.write(new Text(pair.getKey()),new DoubleWritable(pair.getValue()));
    		
    	}
    	System.out.println("DEBUG");
    	
	}
    
    
  }

  public static void main(String[] args) throws Exception {
	
	
    Configuration conf = new Configuration();
    conf.setInt("recordNumber", 0); //
    conf.setInt("numAttribute", 0);
    conf.set("candidate-Key", "candidate-Key");
    conf.set("equivalent-Key", "equivalent-Key");
    conf.set("FD", "FD");
    conf.set("dependant-Key", "dependant-Key");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TestCsv.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);  //error in write context
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }


}