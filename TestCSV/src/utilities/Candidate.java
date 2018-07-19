package utilities;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class Candidate {  //This class is intended like a Candidate

	//The position was the number of attribute for tracking attribute
	
	//private String value;
	//private ObjectArrayList<String> values = null;
	
	String position = null; //Map the position of attribute in row
	int level = 1;
	Object2ObjectOpenHashMap<String,Integer> HMcj = null;
	
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
	
	public int[] parseColumns() {
		
		int[] column = new int[this.level];
		if(level == 1) {
			column[0] = Integer.parseInt(position);
			return column;
		}else {
			String[] columnString = this.position.split(",");  // position = 1
			for(int i=0; i<columnString.length; i++) {
				column[i] = Integer.parseInt(columnString[i]);
			}
			return column;
		}
	}
	
	public Object2ObjectOpenHashMap<String,Integer> getMap() {
		return HMcj;
	}
	
	public String toString() {
		return position;
	}
	
}
