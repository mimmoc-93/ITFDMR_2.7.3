package test;

import java.util.ArrayList;

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
		int[] aaa = new int[10];
		aaa[0] = 1;
		aaa[1] = 2;
		for(int i=0; i<aaa.length; i++) {
			System.out.println(aaa[i]);
		}
        
	}

	public static ArrayList<String> getCombinations(String[] text) {
	    ArrayList<String> results = new ArrayList<String>();
	    for (int i = 0; i < text.length; i++) {
	        // Record size as the list will change
	        int resultsLength = results.size();
	        for (int j = 0; j < resultsLength; j++) {
	            results.add(text[j] + results.get(j));
	        }
	        results.add(text[i]);
	    }
	    return results;
	}
	
	static ArrayList<Candidate> printSubsets(String set[])
    {
		ArrayList<Candidate> result = new ArrayList<Candidate>();
        int n = set.length;
 
        // Run a loop for printing all 2^n
        // subsets one by obe
        for (int i = 0; i < (1<<n); i++)
        {
        	Candidate tmp = new Candidate();
            //System.out.print("{ ");
 
            // Print current subset
            for (int j = 0; j < n; j++)
 
                // (1<<j) is a number with jth bit 1
                // so when we 'and' them with the
                // subset number we get which numbers
                // are present in the subset and which
                // are not
                if ((i & (1 << j)) > 0) {
                    //System.out.print(set[j] + " ");
                    tmp.addAttribute(set[j]);
                }
            result.add(tmp);
        }
        return result;
    }
	
	
	
}
