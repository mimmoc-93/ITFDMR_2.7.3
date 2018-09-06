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


public class InitialMapTask {

  public static class InitialMapper extends Mapper<Object, Text, Text, MapWritable>{

    MapWritable HM_ToReturn = null;
    Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>> maplevel = null;
    int numAttribute;
    
    @Override
    protected void setup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    	super.setup(context);
    	numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));  //Il numero di attributi è passato in input dall'utente
    	System.out.println("\n\n******************************************************************\n"
    			+ "*****    Starting Initial Map Task  *********\n"
    			+ "*****    Attribute Number: "+numAttribute+"  ****\n"
    			+ "******************************************************************\n\n");
    	
    	/*
    	 * Creation of the level. 
    	 * If level are not created yet create it
    	*/
    	if(maplevel == null) {  
    		
    		maplevel = Utility.generateCandidateList(numAttribute);
    		
    		System.out.println("**** Generating Level ... **** \n\n");
    		
    		
    		System.out.println("   Livello 1 \n");
            ObjectArrayList<Candidate> tmp = maplevel.get("1");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\n");
            
            System.out.println("   Livello 2 \n");
            tmp = maplevel.get("2");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\n");
            
            System.out.println("   Livello n \n");
            tmp = maplevel.get("n");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\n");
            
            
            System.out.println("   Livello n-1 \n");
            tmp = maplevel.get("n-1");
            for(int i=0; i<tmp.size(); i++) {
            	System.out.print(tmp.get(i).toString()+" - ");  
            }
            System.out.println("\n\n");
    		
            System.out.println("\n\n Level Generated!!!! \n\n");
    	}
    	
    	
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
      StringTokenizer itr = new StringTokenizer(value.toString()); //record in input
      
      while (itr.hasMoreTokens()) { 
        String record = itr.nextToken();
        String[] recordSplit = record.split(",");
        
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
    	 * Finish Partial PMF
    	 */
    	//System.out.println("DEBUGGGG");
      }
  	

    }
    
    @Override
    protected void cleanup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    	/*
    	 * Write Partial PMF in Output to HDFS
    	 */
    	
    	
    	for(ObjectArrayList<Candidate> level: maplevel.values()) {
    		
    		for(int i=0; i<level.size(); i++) { //level
    			HM_ToReturn = new MapWritable();
    			//candidate
    			Candidate tmp = level.get(i);
    			Text candidate = new Text(tmp.toString());  //Write to HDFS
    			
    			Iterator it = tmp.getIterator();
    			while(it.hasNext()) {
    				
    				Map.Entry<String, Integer> pair = (Entry<String,Integer>) it.next();
    				Text chiave = new Text(pair.getKey());
    				IntWritable valore = new IntWritable(pair.getValue());
    				
    				HM_ToReturn.put(chiave, valore);
    				
    			}
    			context.write(candidate, HM_ToReturn);
    		}
    	
    	}
    	
    	System.out.println("***********************************************\n"
    			+ "*****  Terminating the InitialMapTask  ********\n"
    			+ "***********************************************\n\n");
    	
    }
    
  }

}