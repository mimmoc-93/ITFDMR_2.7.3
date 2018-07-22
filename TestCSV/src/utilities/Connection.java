package utilities;

public class Connection {

	String cand1;
	String cand2;
	
	Double entropy1;
	Double entropy2;
	
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
		ret+=""+cand1+" -> "+cand2;
		return ret;
	}
	
	public String toString() {
		String toRet = ""+cand1+" "+cand2;
		return toRet;
	}
	
}
