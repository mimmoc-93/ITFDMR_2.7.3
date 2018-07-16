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


public class TestCsv {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, MapWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    MapWritable HM_ToReturn = new MapWritable();
    
    @Override
    protected void setup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String record = itr.nextToken();
        String[] recordSplit = record.split(",");
    	Object2ObjectOpenHashMap<String,ObjectArrayList<Attribute>> mapLevel = Utility.generateCandidateList(record);
    	/*
        System.out.println("\n\nLivello 1 \n\n");
        ObjectArrayList<Attribute> tmp = mapLevel.get("1");
        for(int i=0; i<tmp.size(); i++) {
        	System.out.print(tmp.get(i).toString()+" - ");  
        }
        System.out.println("\n\nLivello 2 \n\n");
        tmp = mapLevel.get("2");
        for(int i=0; i<tmp.size(); i++) {
        	System.out.print(tmp.get(i).toString()+" - ");  
        }
        System.out.println("\n\nLivello n \n\n");
        tmp = mapLevel.get("n");
        for(int i=0; i<tmp.size(); i++) {
        	System.out.print(tmp.get(i).toString()+" - ");  
        }
        System.out.println("\n\nLivello n-1 \n\n");
        tmp = mapLevel.get("n-1");
        for(int i=0; i<tmp.size(); i++) {
        	System.out.print(tmp.get(i).toString()+" - ");  
        }
        */
    	
    	ObjectArrayList<Attribute> level_ = mapLevel.get("1");
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
    	}
    	
    	level_ = mapLevel.get("2");
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
    		context.write(new Text(level_.get(i).toString()), HM_ToReturn);; //EMIT (cj, HMcj)
    	}
    	
    	
    	
      }
    }
    
    @Override
    protected void cleanup(
			Mapper<Object, Text, Text, MapWritable>.Context context)
					throws IOException, InterruptedException {
    	
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,MapWritable,Text,IntWritable> {
    
    
    
    public void reduce(Text key, Iterable<MapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      System.out.println("------------------>"+key+"\n----->");
      for (MapWritable value : values) {

          Iterator it = value.entrySet().iterator();
          while(it.hasNext()) {
  			Map.Entry<Text,IntWritable> pair = (Entry<Text, IntWritable>) it.next();
  			
  	        System.out.println(pair.getKey().toString() + " = " + pair.getValue());
  	        it.remove(); // avoids a ConcurrentModificationException
  		}

          
      }
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