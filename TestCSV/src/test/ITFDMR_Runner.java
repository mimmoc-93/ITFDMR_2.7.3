package test;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.InitialMapTask.InitialMapper;
import test.InitialReduceTask.InitialReducer;
import test.IterativeMapTask.IterativeMapper;
import test.IterativeReduceTask.IterativeReducer;


public class ITFDMR_Runner extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ITFDMR_Runner(), args);
        System.exit(res);
    }
	
	public int run(String[] args) throws Exception {
		
		 Configuration conf = this.getConf();
		 
		 String numAttribute = args[2];
		 conf.set("numAttribute", numAttribute);
		 
		 Job job = Job.getInstance(conf, "ITFDMR");
		 job.setJarByClass(ITFDMR_Runner.class);
		 
		 job.setMapperClass(InitialMapper.class);
		 job.setReducerClass(InitialReducer.class);
		 //job.setCombinerClass(InitialReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(MapWritable.class);
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(DoubleWritable.class);
		 
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]+"/temp"));
		 
		 //MultipleOutputs.addNamedOutput(job, "configuration", TextOutputFormat.class, NullWritable.class, Text.class);
		 
		 //return job.waitForCompletion(true) ? 0 : 1;
		
		 job.waitForCompletion(true);
		 
		 int iterationToDo = Integer.parseInt(numAttribute) - 3 ;
		 //iterationToDo = 0;
		 String[] dirTemp = new String[iterationToDo];
		 for(int i=0; i< dirTemp.length; i++) {
			 if(i == 0) {
				 dirTemp[i] = "/temp";
			 }else
				 dirTemp[i] = "/"+String.valueOf(i);
		 }
		 
		 for(int i=0 ; i<dirTemp.length; i++) {
			 System.out.println("------------------------------"+dirTemp[i]);
		 }
		 
		 
		 int count = 0;
		 while(count < iterationToDo) {
			 
			 conf.setInt("Iteration", count);
			 
			 Job job2 = Job.getInstance(conf, "ITFDMR_task2");
			 job2.setJarByClass(ITFDMR_Runner.class);
			 
			 job2.setMapperClass(IterativeMapper.class);
			 job2.setReducerClass(IterativeReducer.class);
				 
			 job2.setMapOutputKeyClass(Text.class);
			 job2.setMapOutputValueClass(MapWritable.class);
			 job2.setOutputKeyClass(Text.class);
			 job2.setOutputValueClass(DoubleWritable.class);
			 
			 //To Generalize with $HADOOP_HOME variable
			 conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));  //for, hdfs distribuited cache
			 conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			 
			 FileSystem fs = FileSystem.get(conf);
			 FileStatus[] fileList = fs.listStatus(new Path(args[1]+dirTemp[count]) , 
	                 new PathFilter(){
	                       @Override public boolean accept(Path path){
	                              return path.getName().startsWith("part-");
	                       } 
	                  } );
			 
			 System.out.println("\n\n");
			 
			 for(int i=0; i < fileList.length;i++){ 
				 
	             job2.addCacheFile(fileList[i].getPath().toUri());
				 System.out.println("***********************  "+fileList[i].getPath().toString());
			 
			 }
			 
			 System.out.println("\n\n");
			 
			 FileInputFormat.addInputPath(job2, new Path(args[0]));
			 FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/"+(count+1)));
			 
			 job2.waitForCompletion(true);
			 
			 count++;
		 }
		 
		 
		 return 0;
		 
	}
	
}
