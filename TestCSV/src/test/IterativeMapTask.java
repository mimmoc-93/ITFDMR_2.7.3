package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import utilities.Candidate;
import utilities.Utility;

public class IterativeMapTask {

	public static class IterativeMapper extends Mapper<Object, Text, Text, MapWritable>{
		
		int numAttribute;
		int level;
		Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>> maplevel = null;
		ObjectArrayList<Candidate> levelkminus1 = null;
		ObjectArrayList<Candidate> levelk = null;
		
		@Override
	    protected void setup(
				Mapper<Object, Text, Text, MapWritable>.Context context)
						throws IOException, InterruptedException {
	    	
	    	super.setup(context);
	    	numAttribute = Integer.parseInt(context.getConfiguration().get("numAttribute"));
	    	
	    	int iteration = Integer.parseInt(context.getConfiguration().get("Iteration"));
	    	
	    	
	    	System.out.println("\n\n***********************************************\n"
	    			+ "******  Starting Iterative Map Task **************\n"
	    			+ "****** Num Attribute = "+numAttribute+"  *******\n"
	    			+ "*************************************************\n"
	    			+ "****** Iterazione Numero = "+iteration+"  *******");
	    	
	    	maplevel = new Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>>();
	    	
	    	levelkminus1 = new ObjectArrayList<Candidate>();
	    	URI[] cacheFiles= context.getCacheFiles();
	        if(cacheFiles != null) {
	            for (URI cacheFile : cacheFiles) {
	                level = readFile(cacheFile,context,levelkminus1);
	            }
	        }
	        maplevel.put(String.valueOf(level), levelkminus1);
	        
	        System.out.println("Livello "+level+"   ");
	        for(int i=0; i<levelkminus1.size(); i++) {
	        	System.out.println(levelkminus1.get(i));
	        }
	        System.out.println("---------------------");
	        
	        /*
	         * Generating level k
	         */
	       
	        levelk = Utility.generateCandidateList(numAttribute,level,levelk);
	        maplevel.put(String.valueOf(level+1), levelk);
	        
	        System.out.println("Livello "+(level+1)+" ");
	        for(int i=0; i<levelk.size(); i++) {
	        	System.out.println(levelk.get(i));
	        }
	        System.out.println("------------------------");
	    	
	    }
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) { 
				String record = itr.nextToken();
		        String[] recordSplit = record.split(",");
		        
		        String[] level_to_compute = {String.valueOf(level+1)};
		        
		        /*
		         * Find Partial PMF in level k = level+1
		         */
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
			}
			System.out.println("DEBUGGGGGG");
			
		}
		
		protected void cleanup(
				Mapper<Object, Text, Text, MapWritable>.Context context)
						throws IOException, InterruptedException {
			
		}
		
			
		private int readFile(URI fileURI,Context context,ObjectArrayList<Candidate> levelkminus1) {
			
			int numLevel = 0;
			
			context.getConfiguration().addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			context.getConfiguration().addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			
			System.out.println(fileURI.toString());
			Path pt=new Path("hdfs://localhost:9000/user/user/OUTPUT/temp/part-r-00000");
			
			BufferedReader br = null;
			
			try {
				
				FileSystem fs = FileSystem.get(context.getConfiguration());
				br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				
				String line;
				line=br.readLine();
				
				while (line != null){
					
		            String[] lineSplit = line.split("\\s+");
		            
		            Candidate tmp = new Candidate(lineSplit[0]);
		            numLevel = tmp.getLevel();
		            levelkminus1.add(tmp);
		            
		            
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
