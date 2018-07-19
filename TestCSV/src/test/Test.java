package test;

import java.util.ArrayList;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import utilities.Candidate;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		String set[] = {"a", "b", "c","d"};
		ArrayList<Candidate> result = printSubsets(set);
		for(int i=0; i<result.size(); i++) {
			ObjectArrayList<String> tmp = result.get(i).getValues();
			for(int j=0; j<tmp.size(); j++) {
				System.out.print(tmp.get(j)+" ");
			}
			System.out.println();
		}
		*/
		
		Object2ObjectOpenHashMap<String,Integer> HMcj = new Object2ObjectOpenHashMap<String,Integer>();
		HMcj.put("mimmo",2);
		HMcj.put("francesco", 1);
		HMcj.put("dave", 1);
		HMcj.put("egidio", 3);
		
		double e = 0.0;
		int size = getTotalRecord(HMcj);
		System.out.println("size = "+size);
		for (Map.Entry<String, Integer> entry : HMcj.entrySet()) {
		      String cx = entry.getKey();
		      double p = (double) entry.getValue() / size;
		      e += p * log2(p);
		}
		e = -e;
		
		
		System.out.println("L'entropia è :"+e);
        
	}

	private static double log2(double a) {
	    return Math.log(a) / Math.log(2);
	  }
	
	private static int getTotalRecord(Object2ObjectOpenHashMap<String,Integer> HMcj) {
		int count =0;
		for(int value: HMcj.values()) {
			count+=value;
		}
		return count;
	}
	
}
