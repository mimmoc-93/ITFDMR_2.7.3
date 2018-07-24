package utilities;

import test.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class Utility {

	public static ObjectArrayList<Candidate> generateCandidateList(int attribute, int level, ObjectArrayList<Candidate> levelk) {  
		
		levelk = new ObjectArrayList<Candidate>();
		  
		int numAttribute = attribute;
		//array support for permutation
		int[] support = new int[attribute];
		for(int i=0; i<numAttribute; i++) {
			support[i] = i;
		}
		
		ObjectArrayList<Candidate> listLevelTemp = null;
		
		levelk = printCombination(support, numAttribute, level+1, listLevelTemp);
		
		return levelk;
		
	}
	
	public static Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>> generateCandidateList(int attribute) {  
		
		Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>> mapLevel = new Object2ObjectOpenHashMap<String,ObjectArrayList<Candidate>>();; //level{("1","list"),("n-1","list")}
		//split
  
		int numAttribute = attribute;
		//array support for permutation
		int[] support = new int[numAttribute];
		for(int i=0; i<numAttribute; i++) {
			support[i] = i;
		}
		
		ObjectArrayList<Candidate> listLevelTemp = null;
		
		//--------------Generazione dei livelli 1,2,n,n-1
		mapLevel.put("1", printCombination(support,numAttribute,1,listLevelTemp));
		mapLevel.put("2", printCombination(support,numAttribute,2,listLevelTemp));
		mapLevel.put("n-1", printCombination(support,numAttribute,numAttribute-1,listLevelTemp));
		mapLevel.put("n", printCombination(support,numAttribute,numAttribute,listLevelTemp));
		
		return mapLevel;
		
	}
	
    public static void combinationUtil(int arr[], int n, int r, int index, int data[], int i, ObjectArrayList<Candidate> toReturn) {
    	// Current combination is ready to be printed, 
    	// print it
    	if (index == r) {
    		String toWrite = "";
    		for (int j = 0; j < r; j++) {
    			//System.out.print(data[j] + " ");//stampa la riga
    			toWrite+=data[j]+",";
    			//System.out.println(toWrite);
    		}
    		Candidate tmp = new Candidate(toWrite.substring(0, toWrite.length()-1));
    		toReturn.add(tmp);
    		//System.out.println(toWrite);   //invio, inserimento all'interno dell array
    		return;
    	}

    	if (i >= n)
    		return;

    	data[index] = arr[i];
    	combinationUtil(arr, n, r, index + 1, 
                 data, i + 1, toReturn);


    	combinationUtil(arr, n, r, index, data, i + 1, toReturn);
    }
	
    public static ObjectArrayList<Candidate> printCombination(int arr[], int n, int r,ObjectArrayList<Candidate> toReturn) {
        
    	toReturn = new ObjectArrayList<Candidate>();
    	
        int data[] = new int[r];
        
        combinationUtil(arr, n, r, 0, data, 0, toReturn);
        
        return toReturn;
    }
	
    public static double log2(double a) { //log2
	    return Math.log(a) / Math.log(2);
	}
    
    public static double arrotonda(double d, int p) {
		return Math.rint(d*Math.pow(10,p))/Math.pow(10,p);
	} 
}
