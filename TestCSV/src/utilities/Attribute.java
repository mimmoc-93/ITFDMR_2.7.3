package utilities;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class Attribute {  //This class is intended like a Candidate

	private String value;
	private ObjectArrayList<String> values = null;
	private boolean single;
	
	public Attribute() {
		values = new ObjectArrayList<>();
		single = false;
	}
	
	public Attribute(String value) {
		this.value = value;
		single = true;
	}
	
	public Attribute(ObjectArrayList<String> values) {
		this.values = values;
		if(this.values.size() > 1)
			single=false;
	}
	
	public boolean isSingle() {
		return single;
	}
	
	public String getValue() {
		return value;
	}
	
	public void addAttribute(String att) {
		values.add(att);
	}
	
	public ObjectArrayList<String> getValues() {
		return values;
	}
	
	public String toString() {
		StringBuilder ret = new StringBuilder();
		if(values!=null) {
			ret.append("(");
			for(int i=0; i<values.size(); i++) {
				ret.append(values.get(i)+",");
			}
			String ritorno = ret.toString();
			return ritorno.substring(0, ritorno.length()-1)+")";
		}else
			return "("+value+")";
	}
		
	
}
