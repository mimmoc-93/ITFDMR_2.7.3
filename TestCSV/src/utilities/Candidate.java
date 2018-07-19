package utilities;

import java.util.Iterator;
import java.util.Map;

import JavaMI.Entropy;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class Candidate {  //This class is intended like a Candidate

	//The position was the number of attribute for tracking attribute
	
	//private String value;
	//private ObjectArrayList<String> values = null;
	
	String position = null; //Map the position of attribute in row
	int level = 1;
	Object2ObjectOpenHashMap<String,Integer> HMcj = null;
	double entropy;
	
	public Candidate(String pos) {
		this.position = pos;    //    (1,2,3)
		calculateLevel();
		this.HMcj = new Object2ObjectOpenHashMap<String,Integer>();
	}
	
	public void calculateLevel() {
		String[] s=this.position.split(",");
		if(s.length == 0) {
			this.level = 1;
		}else 
			this.level = s.length;
		
	}
	
	public void put(String s,Integer i){
		HMcj.put(s, i);
	}
	
	public int get(String s) {
		if(HMcj.get(s) == null )
			return 0;
		else
			return HMcj.get(s);
	}
	
	public void addAttribute(String pos) {
		position+=","+pos;
		calculateLevel();
	}
	
	public int getLevel() {
		return this.level;
	}
	
	public String getTupla(String[] recordSplit) {
		String tupla = "";
		
		if(!position.contains(",")) {
			return recordSplit[Integer.parseInt(position)];
		}
		
		String[] positionSplitted= position.split(",");
		
		for(int j=0 ; j<positionSplitted.length; j++) {
			tupla += recordSplit[Integer.parseInt(positionSplitted[j])] +"," ;
		}
		tupla = tupla.substring(0, tupla.length()-1);
		return tupla;
	}
	
	
	public int getTotalRecord() {
		
		int count =0;
		for(int value: HMcj.values()) {
			count+=value;
		}
		return count;
	}
	
	public void calculateEntropy(int total) {
		
		int size = total;
		double e = 0.0;
		for (Map.Entry<String, Integer> entry : HMcj.entrySet()) {
		      String cx = entry.getKey();
		      double p = (double) entry.getValue() / size;
		      e += p * Utility.log2(p);
		}
		this.entropy = -e;
	}
	
	public void calculateEntropy() {
		
		int size = getTotalRecord();
		double e = 0.0;
		for (Map.Entry<String, Integer> entry : HMcj.entrySet()) {
		      String cx = entry.getKey();
		      double p = (double) entry.getValue() / size;
		      e += p * Utility.log2(p);
		}
		this.entropy = -e;
	}
	
	public Iterator getIterator() {
		return HMcj.entrySet().iterator();
	}
	
	public Object2ObjectOpenHashMap<String,Integer> getMap() {
		return HMcj;
	}
	
	public double getEntropy() {
		return entropy;
	}
	
	public String toString() {
		return position;
	}
	
}
