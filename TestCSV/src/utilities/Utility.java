package utilities;

import test.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
    
    public static boolean minimalFD(String LHS, String RHS, String FD) {
		
    	String LHS_FD;
		String RHS_FD;
		
		LHS_FD = FD.substring(0,FD.indexOf("-")).trim();
		
		
		RHS_FD = FD.substring(FD.indexOf("-")+2,FD.length()).trim();
		
		
		
		String[] LHS_split = LHS.split(",");
		String[] RHS_split = RHS.split(",");
		String[] LHS_FD_split = LHS_FD.split(",");
		String[] RHS_FD_split_old = RHS_FD.split(",");
		
		Set<String> Union = new HashSet<String>();
		for(int i=0; i<LHS_FD_split.length; i++)
			Union.add(LHS_FD_split[i]);
		for(int i=0; i<RHS_FD_split_old.length; i++)
			Union.add(RHS_FD_split_old[i]);
		
		RHS_FD = "";
		
		Iterator it = Union.iterator();
		while(it.hasNext()) {
			String tmp = (String) it.next();
			RHS_FD+=tmp+",";
		}
		
		String[] RHS_FD_split = RHS_FD.split(",");
		
		int countLHS = 0;
		int countRHS = 0;
		String differenceLHS = "";
		String differenceRHS = "";
		
		for(int i=0; i<LHS_split.length; i++) {   //Find the difference between LHS in FD and LHS in input
			boolean find=false;
			for(int j=0; j<LHS_FD_split.length; j++) {
				if(LHS_split[i].equals(LHS_FD_split[j])) {
					countLHS++;
					find=true;
					break;
				}
			}
			if(!find)
				differenceLHS+=LHS_split[i]+",";
		}
		
		
		
		if(countLHS == LHS_FD_split.length) {  //Find the difference between RHS in FD and RHS in input
			for(int i=0; i<RHS_split.length; i++) {
				boolean find=false;
				for(int j=0; j<RHS_FD_split.length; j++) {
					if(RHS_split[i].equals(RHS_FD_split[j])) {
						countRHS++;
						find=true;
						break;
					}
				}
				if(!find)
					differenceRHS+=RHS_split[i]+",";
			}
		}
		
		
		if(countLHS == LHS_FD_split.length && countRHS == RHS_FD_split.length && differenceLHS.equals(differenceRHS)) {
			return true;
		}else
			return false;
		
	}
}
