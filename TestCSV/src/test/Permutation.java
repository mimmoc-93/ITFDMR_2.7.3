package test;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import utilities.Candidate;

class Permutation {
	 
    
    public void combinationUtil(int arr[], int n, int r,
                          int index, int data[], int i, ObjectArrayList<Candidate> toReturn)
    {
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
 
    
    public  ObjectArrayList<Candidate> printCombination(int arr[], int n, int r,ObjectArrayList<Candidate> toReturn)
    {
        
        int data[] = new int[r];
 
        
        combinationUtil(arr, n, r, 0, data, 0, toReturn);
        
        return toReturn;
    }
 
    
    public static void main(String[] args)
    {
        int arr[] = { 1,2,3,4 };
        int r = 3;
        int n = arr.length;
        
        ObjectArrayList<Candidate> TMP = new ObjectArrayList<Candidate>();
        
        printCombination(arr, n, r, TMP);
        
        for(int i=0; i<TMP.size();i++) {
        	System.out.println(TMP.get(i).toString());
        }
    }
}