package utilities;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.TreeMap;

import ch.obermuhlner.math.big.BigDecimalMath;

public class EntropyCalculator {

	private static int count;
    private static Map<String,Integer> amountMap;
    private static Map<String,BigDecimal> probabilityMap;
	MathContext mathcontext = new MathContext(100);
    
    public EntropyCalculator(int count, Map<String,Integer> aMap) {
    	
    	this.count = count;
    	this.amountMap = aMap;
    	probabilityMap = new TreeMap<String,BigDecimal>();
    	
    }
    
    
    public BigDecimal computeEntropy() {
    	
    	probabilityMap = probabilityCalc();
    	return entropyCalc();
    }
    
    public Map<String,BigDecimal> probabilityCalc() {
    	
    	BigDecimal pro = new BigDecimal(0);
    	BigDecimal bcount = new BigDecimal(count);
    	
    	for(Map.Entry<String, Integer> entry: amountMap.entrySet()) {
    		
    		BigDecimal value = new BigDecimal(entry.getValue());
    		pro = value.divide(bcount,mathcontext); 
    		probabilityMap.put(entry.getKey(), pro);
    		
    	}
    	
    	return probabilityMap;
    }
    
    public BigDecimal entropyCalc() {
    	
    	MathContext mathcontext = new MathContext(100);
    	BigDecimal entropy = new BigDecimal(0);
    	
    	for(BigDecimal  p : probabilityMap.values()) {
    		//System.out.println("*******  "+p+"  *********");
    		//entropy += -(p * Math.log(p) / Math.log(2));
    		
    		BigDecimal log2 = BigDecimalMath.log(p, mathcontext).divide(BigDecimalMath.log(new BigDecimal(2), mathcontext),mathcontext);
    		BigDecimal multiply = p.multiply(log2, mathcontext);
    		BigDecimal multiplyabs = multiply.abs(mathcontext);
    		entropy = entropy.add(multiplyabs, mathcontext);
    	}
    	
    	//System.out.println("--------  "+entropy+"  ----------");
    	
    	//System.out.println(entropy);
    	return entropy;
    }
    
    
}
