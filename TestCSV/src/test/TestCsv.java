package test;
import utilities.*;

import it.unimi.dsi.fastutil.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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

    MapWritable HM_ToReturn = new MapWritable();
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
        
        //if level are not created, create it
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
        
    	
        
        
    	
    	//   Computing---------------------------------------------------------------------------------------------------
    	ObjectArrayList<Candidate> level_ = maplevel.get("1");
    	
    	/*
    	for(int i=0; i<level_.size(); i++) {
    		
    		Object2ObjectOpenHashMap<String,Integer> HMcj = new Object2ObjectOpenHashMap<String,Integer>();
    		for(int j=0; j< recordSplit.length; j++) {
    			String current = recordSplit[j];
    			if(HMcj.get(current) == null) {
    				HMcj.put(current, 1);
    			}else {
    				HMcj.put(current, HMcj.get(current)+1);
    			}
    		}
    		//EMIT(cj,HMcj)
    		
    		System.out.println("Candidate is: "+level_.get(i));
    		Iterator it = HMcj.entrySet().iterator();
    		while(it.hasNext()) {
    			Map.Entry<String,Integer> pair = (Entry<String, Integer>) it.next();
    			HM_ToReturn.put(new Text(pair.getKey().toString()) , new IntWritable(pair.getValue()));
    	        System.out.println(pair.getKey().toString() + " = " + pair.getValue());
    	        it.remove(); // avoids a ConcurrentModificationException
    		}
    		System.out.println("\n\n");
    		context.write(new Text(level_.get(i).toString()), HM_ToReturn); //EMIT (cj, HMcj)
    	}*/
    	
    	
    	//partial pmf for level generic
    	
    	for(int i=0; i<level_.size(); i++) {
    		//candidato-->posizione---->mappare il valore della linea nella HMCJ    1,2 
    		
    		int[] numColumn = level_.get(i).parseColumns(); //   0->1   1->2
    		for(int j=0; j<numColumn.length; j++ ) {
    			if(level_.get(i).get(recordSplit[numColumn[j]]) == 0 ){
    				
    				level_.get(i).put(recordSplit[numColumn[j]], 1);
    				
    			}else {
    				
    				int tmp = level_.get(i).get(recordSplit[numColumn[j]]);
    				level_.get(i).put(recordSplit[numColumn[j]], tmp+1);
    				
    			}
    		}
    	}
    	
    	level_ = maplevel.get("2");
    	for(int i=0; i<level_.size(); i++) {
    		//candidato-->posizione---->mappare il valore della linea nella HMCJ    1,2 
    		
    		String position = level_.get(i).toString();
    		String tupla = "";
    		String[] positionSplitted= position.split(",");
    		for(int j=0 ; j<positionSplitted.length; j++) {
    			tupla += recordSplit[Integer.parseInt(positionSplitted[j])] +"," ;
    		}
    		tupla = tupla.substring(0, tupla.length()-1);
    		//System.out.println(tupla);
    		
    		//int[] numColumn = level_.get(i).parseColumns(); //   0-> 1,2     1->
    		//record da passare a get e put
    		
    			if(level_.get(i).get(tupla) == 0 ){
    				
    				level_.get(i).put(tupla, 1);
    				
    			}else {
    				
    				int tmp = level_.get(i).get(tupla);
    				level_.get(i).put(tupla, tmp+1);
    				
    			}
       	}
    	
    	level_ = maplevel.get("n-1");    	
    	for(int i=0; i<level_.size(); i++) {
    		//candidato-->posizione---->mappare il valore della linea nella HMCJ    1,2 
    		
    		String position = level_.get(i).toString();
    		String tupla = "";
    		String[] positionSplitted= position.split(",");
    		for(int j=0 ; j<positionSplitted.length; j++) {
    			tupla += recordSplit[Integer.parseInt(positionSplitted[j])] +"," ;
    		}
    		tupla = tupla.substring(0, tupla.length()-1);
    		//System.out.println(tupla);
    		
    		//int[] numColumn = level_.get(i).parseColumns(); //   0-> 1,2     1->
    		//record da passare a get e put
    		
    			if(level_.get(i).get(tupla) == 0 ){
    				
    				level_.get(i).put(tupla, 1);
    				
    			}else {
    				
    				int tmp = level_.get(i).get(tupla);
    				level_.get(i).put(tupla, tmp+1);
    				
    			}
       	}
    	level_ = maplevel.get("n");    
    	for(int i=0; i<level_.size(); i++) {
    		//candidato-->posizione---->mappare il valore della linea nella HMCJ    1,2 
    		
    		String position = level_.get(i).toString();
    		String tupla = "";
    		String[] positionSplitted= position.split(",");
    		for(int j=0 ; j<positionSplitted.length; j++) {
    			tupla += recordSplit[Integer.parseInt(positionSplitted[j])] +"," ;
    		}
    		tupla = tupla.substring(0, tupla.length()-1);
    		//System.out.println(tupla);
    		
    		//int[] numColumn = level_.get(i).parseColumns(); //   0-> 1,2     1->
    		//record da passare a get e put
    		
    			if(level_.get(i).get(tupla) == 0 ){
    				
    				level_.get(i).put(tupla, 1);
    				
    			}else {
    				
    				int tmp = level_.get(i).get(tupla);
    				level_.get(i).put(tupla, tmp+1);
    				
    			}
       	}
    	/*
    	level_ = maplevel.get("n-1");
    	for(int i=0; i<level_.size(); i++) {
    		//candidato-->posizione---->mappare il valore della linea nella HMCJ    1,2 
    		
    		int[] numColumn = level_.get(i).parseColumns(); //   0->1   1->2
    		for(int j=0; j<numColumn.length; j++ ) {
    			if(level_.get(i).get(recordSplit[numColumn[j]]) == 0 ){
    				
    				level_.get(i).put(recordSplit[numColumn[j]], 1);
    				
    			}else {
    				
    				int tmp = level_.get(i).get(recordSplit[numColumn[j]]);
    				level_.get(i).put(recordSplit[numColumn[j]], tmp+1);
    				
    			}
    		}
    	}
    	level_ = maplevel.get("n");
    	for(int i=0; i<level_.size(); i++) {
    		//candidato-->posizione---->mappare il valore della linea nella HMCJ    1,2 
    		
    		int[] numColumn = level_.get(i).parseColumns(); //   0->1   1->2   
    		for(int j=0; j<numColumn.length; j++ ) {
    			if(level_.get(i).get(recordSplit[numColumn[j]]) == 0 ){
    				
    				level_.get(i).put(recordSplit[numColumn[j]], 1);
    				
    			}else {
    				
    				int tmp = level_.get(i).get(recordSplit[numColumn[j]]);
    				level_.get(i).put(recordSplit[numColumn[j]], tmp+1);
    				
    			}
    		}
    	}
    	/*
    	
    	
    	for(int i=0; i<level_.size(); i++) {
    		Object2ObjectOpenHashMap<String,Integer> HMcj = new Object2ObjectOpenHashMap<String,Integer>();
    		for(int j=0; j<recordSplit.length; j++) {
    			String current = recordSplit[j];
    			if(HMcj.get(current) == null) {
    				HMcj.put(current, 1);
    			}else {
    				HMcj.put(current, HMcj.get(current)+1);
    			}
    		}
    		//EMIT(cj,HMcj)
    		
    		System.out.println("Candidate is: "+level_.get(i));
    		Iterator it = HMcj.entrySet().iterator();
    		while(it.hasNext()) {
    			Map.Entry<String,Integer> pair = (Entry<String, Integer>) it.next();
    			HM_ToReturn.put(new Text(pair.getKey().toString()) , new IntWritable(pair.getValue()));
    	        System.out.println(pair.getKey().toString() + " = " + pair.getValue());
    	        it.remove(); // avoids a ConcurrentModificationException
    		}
    		System.out.println("\n\n");
    		context.write(new Text(level_.get(i).toString()), HM_ToReturn);; //EMIT (cj, HMcj)
    	}
    	
    	level_ = maplevel.get("n");
    	for(int i=0; i<level_.size(); i++) {
    		Object2ObjectOpenHashMap<String,Integer> HMcj = new Object2ObjectOpenHashMap<String,Integer>();
    		for(int j=0; j<recordSplit.length; j++) {
    			String current = recordSplit[j];
    			if(HMcj.get(current) == null) {
    				HMcj.put(current, 1);
    			}else {
    				HMcj.put(current, HMcj.get(current)+1);
    			}
    		}
    		//EMIT(cj,HMcj)
    		
    		System.out.println("Candidate is: "+level_.get(i));
    		Iterator it = HMcj.entrySet().iterator();
    		while(it.hasNext()) {
    			Map.Entry<String,Integer> pair = (Entry<String, Integer>) it.next();
    			HM_ToReturn.put(new Text(pair.getKey().toString()) , new IntWritable(pair.getValue()));
    	        System.out.println(pair.getKey().toString() + " = " + pair.getValue());
    	        it.remove(); // avoids a ConcurrentModificationException
    		}
    		System.out.println("\n\n");
    		context.write(new Text(level_.get(i).toString()), HM_ToReturn);; //EMIT (cj, HMcj)
    	}
    	
    	
    	for(int i=0; i<level_.size(); i++) {
    		Object2ObjectOpenHashMap<String,Integer> HMcj = new Object2ObjectOpenHashMap<String,Integer>();
    		for(int j=0; j<recordSplit.length; j++) {
    			String current = recordSplit[j];
    			if(HMcj.get(current) == null) {
    				HMcj.put(current, 1);
    			}else {
    				HMcj.put(current, HMcj.get(current)+1);
    			}
    		}
    		
    		
    		System.out.println("Candidate is: "+level_.get(i));
    		Iterator it = HMcj.entrySet().iterator();
    		while(it.hasNext()) {
    			Map.Entry<String,Integer> pair = (Entry<String, Integer>) it.next();
    			HM_ToReturn.put(new Text(pair.getKey().toString()) , new IntWritable(pair.getValue()));
    	        System.out.println(pair.getKey().toString() + " = " + pair.getValue());
    	        it.remove(); // avoids a ConcurrentModificationException
    		}
    		System.out.println("\n\n");
    		context.write(new Text(level_.get(i).toString()), HM_ToReturn);; //EMIT (cj, HMcj)
    	}
    	*/
      }
    }
    
    @Override
    protected void cleanup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    	//context
    	for(ObjectArrayList<Candidate> level: maplevel.values()) {
    		
    		for(int i=0; i<level.size(); i++) { //scorriamo i livelli
    			
    			//scorrere i candidati
    			Candidate tmp = level.get(i);
    			System.out.println("Candidato = -------"+tmp.toString()+"    ---------------");
    			Object2ObjectOpenHashMap<String,Integer> mappa = tmp.getMap();
    			Iterator it = mappa.entrySet().iterator();
    			while(it.hasNext()) {
    				Map.Entry<String, Integer> pair = (Entry<String,Integer>) it.next();
    				System.out.println(pair.getKey()+"  -> "+pair.getValue());
    				HM_ToReturn.put(new Text(pair.getKey()), new IntWritable(pair.getValue()));
    				it.remove();
    			}
    			context.write(new Text(tmp.toString()), HM_ToReturn);
    		}
    		
    	}
    	
		
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,MapWritable,Text,IntWritable> {
    
	Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String,Integer>> recalc_candidate =  new Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String,Integer>>();
	  
    protected void setup(
				Mapper<Object, Text, Text, MapWritable>.Context context)
						throws IOException, InterruptedException {
	    	
	    	System.out.println("\n\n------------------Setup del reduce------------------\n\n");
	    	//I candidati non possono essere creati perchè non si è a conoscenza del numero di attributi
	    	
	    	
	}
	  
	  
    public void reduce(Text key, Iterable<MapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      /*
      
      for (MapWritable value : values) {
    	  
    	  System.out.println("------------------>"+key+"\n----->");
          Iterator it = value.entrySet().iterator();
          while(it.hasNext()) { //H;CJ
  			Map.Entry<Text,IntWritable> pair = (Entry<Text, IntWritable>) it.next();
  			
  			String chiave = pair.getKey().toString();
  			int occorrenza = pair.getValue().get();
  	        //HMcj.put(chiave, occorrenza);
  			System.out.println(pair.getKey().toString() + " = " + pair.getValue());
  	        //double pofv[] = findPofv((Integer)pair.getValue().get());     //Should be String array, but not sure, entropy can be evaluated from all type of value
  	        //Entropy.calculateEntropy(pofv);
  	        
  	        it.remove(); // avoids a ConcurrentModificationException
  	        
  		}
          
          
      }
      
      */
      
      System.out.println("----------------------------------------------------------------------------------------");
      
    }
    
    protected void cleanup(
			Reducer<Text,MapWritable,Text,IntWritable>.Context context)
					throws IOException, InterruptedException {

		//super.setup(context);

		//k = Integer.parseInt(context.getConfiguration().get(Constant.K));

		//hashMapCapacityCombiner = Integer.parseInt(context.getConfiguration().get(Constant.SIZE_HM_REDUCE));
    	System.out.println("clueanup");
	}
    
    
  }

  public static void main(String[] args) throws Exception {
	  
    Configuration conf = new Configuration();
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