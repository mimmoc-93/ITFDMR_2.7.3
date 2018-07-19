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
        
        /*
    	 * Creation of the level. 
    	 * If level are not created yet create it
    	*/
    	if(maplevel == null) {  
    		
    		maplevel = Utility.generateCandidateList(record);
    		
    		System.out.println("Generazione dei livelli-------------- \n\n");
    		
    		
    		System.out.println("\n\nLivello 1 \n\n");
            ObjectArrayList<Candidate> tmp = maplevel.get("1");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
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
    	
    	int currentLevel=1;
    	
    	//context
    	
    	for(ObjectArrayList<Candidate> level: maplevel.values()) {
    		//System.out.println("**************Livello "+currentLevel+"    *******************************");
    		
    		for(int i=0; i<level.size(); i++) { //scorriamo i livelli
    			HM_ToReturn = new MapWritable();
    			//scorrere i candidati
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
    		currentLevel++;
    		//System.out.println("*********************************************************************");
    		
    	}
    	
    	
    	/*
    	ObjectArrayList<Candidate> level = maplevel.get("1");
    	System.out.println("**************Livello "+currentLevel+"    *******************************");
		for(int i=0; i<level.size(); i++) { //scorriamo i livelli
			HM_ToReturn = new MapWritable();
			//scorrere i candidati
			Candidate tmp = level.get(i);
			Text candidate = new Text(tmp.toString());
			System.out.println("------- Candidato = "+candidate+"    ---------------");
			Iterator it = tmp.getIterator();
			while(it.hasNext()) {
				Map.Entry<String, Integer> pair = (Entry<String,Integer>) it.next();
				Text chiave = new Text(pair.getKey());
				IntWritable valore = new IntWritable(pair.getValue());
				
				System.out.println(chiave+" -> "+valore);
				HM_ToReturn.put(chiave, valore);
				//it.remove();
			}
			context.write(candidate, HM_ToReturn);
		}
		System.out.println("*********************************************************************");
		
		level = maplevel.get("1");
		System.out.println("**************Livello "+currentLevel+"  Una nuova mappa  *******************************");
		for(int i=0; i<level.size(); i++) { //scorriamo i livelli
			HM_ToReturn = new MapWritable();
			//scorrere i candidati
			Candidate tmp = level.get(i);
			Text candidate = new Text(tmp.toString());
			System.out.println("------- Candidato = "+candidate+"    ---------------");
			//Object2ObjectOpenHashMap<String,Integer> mappa = tmp.getMap();
			//System.out.println("Size della mappa "+ mappa.size()+"  ");
			Iterator it = tmp.getIterator();
			
			while(it.hasNext()) {
				Map.Entry<String, Integer> pair = (Entry<String,Integer>) it.next();
				Text chiave = new Text(pair.getKey());
				IntWritable valore = new IntWritable(pair.getValue()+2);
				
				System.out.println(chiave+" -> "+valore);
				HM_ToReturn.put(chiave, valore);
			}
			context.write(candidate, HM_ToReturn);
		}
		currentLevel++;
		System.out.println("*********************************************************************");
		
    	*/
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,MapWritable,Text,IntWritable> {
    
	ObjectArrayList<Candidate> candidateList = new ObjectArrayList<Candidate>();  //lista di tutti i candidati ricevuti
	  
	  
    protected void setup(
				Reducer<Text, MapWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
    	
    	super.setup(context);
	
    }
	  
	  
    public void reduce(Text key, Iterable<MapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      Candidate candidato = new Candidate(key.toString());  //ricreazione del candidato      
      
      for (MapWritable value : values) {  //for every key=candidate findPOFV, total pmf, from values map in input
    	  
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
      
      
      //calcolateEntropy
  
      int recordNumber  = Integer.parseInt(context.getConfiguration().get("recordNumber"));
      if(recordNumber == 0) {
    	  int newRecord = candidato.getTotalRecord();
    	  context.getConfiguration().set("recordNumber", String.valueOf(newRecord));
      }
      int newRecord = Integer.parseInt(context.getConfiguration().get("recordNumber"));
      
      candidato.calculateEntropy(newRecord);
      System.out.println("Entropy of Candidate "+candidato.toString()+" is: "+candidato.getEntropy()+"  \n\n");
      candidateList.add(candidato); //Add candidate to list of all candidate;
      System.out.println("---------------------------------------------------------------------------------------- \n\n");//DEBUG
      
    }
    
    protected void cleanup(
			Reducer<Text,MapWritable,Text,IntWritable>.Context context)
					throws IOException, InterruptedException {

		//super.setup(context);

		//k = Integer.parseInt(context.getConfiguration().get(Constant.K));

		//hashMapCapacityCombiner = Integer.parseInt(context.getConfiguration().get(Constant.SIZE_HM_REDUCE));
    	
	}
    
    
  }

  public static void main(String[] args) throws Exception {
	
	
    Configuration conf = new Configuration();
    conf.setInt("recordNumber", 0);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TestCsv.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }


}