package utilities;

public class Connection {

	public String cand1;
	public String cand2;
	public String[] cand1split;
	public String[] cand2split;
	
	public Double entropy1;
	public Double entropy2;
	
	public Connection(String cand1, String cand2, Double entropy1, Double entropy2) {
		
		this.cand1 = cand1;
		this.cand2 = cand2;
		this.entropy1 = entropy1;
		this.entropy2 = entropy2;
		
	}
	
	public boolean check() {
		
		if(entropy1.equals(entropy2))
			return true;
		else
			return false;	
		
	}
	
	public String getFD() {  //TODO better
		String ret = "";
		ret+=""+cand1+"->"+cand2;
		return ret;
	}
	
	public String getFD2() {
		String[] cand1split = cand1.split(",");
		String[] cand2split = cand2.split(",");
		
		String ret1 = "";
		
		for(int i=0; i<cand2split.length; i++) {
			
			for(int j=0; j<cand1split.length; j++) {
				
				if(cand2split[i].equals(cand1split[j])) {
					ret1+=cand2split[i]+",";
					cand1split[j] = "";
					break;
				}
				
			}
		}
		
		String ret2 = "";
		for(int i=0; i<cand1split.length; i++) {
			if(!cand1split[i].equals(""))
				ret2+=cand1split[i]+",";
		}
		
		
		return ret1.substring(0,ret1.length()-1)+"->"+ret2.substring(0,ret2.length()-1);
	}
	
	public String toString() {
		String toRet = ""+cand1+" "+cand2;
		return toRet;
	}
	
}
