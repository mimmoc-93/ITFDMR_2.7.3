package test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IterativeReduceTask {

	public static class IterativeReducer
    extends Reducer<Text,MapWritable,Text,DoubleWritable> {
		
		protected void setup(
				Reducer<Text, MapWritable, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {
			
			super.setup(context);
			
			
		}
		
		public void reduce(Text key, Iterable<MapWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			
		}
		
		protected void cleanup(
				Reducer<Text,MapWritable,Text,DoubleWritable>.Context context)
						throws IOException, InterruptedException {
			
		}
		
	}
	
}
