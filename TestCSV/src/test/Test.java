package test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import utilities.Candidate;
import utilities.Utility;

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
		/*
		Object2ObjectOpenHashMap<String,Integer> HMcj = new Object2ObjectOpenHashMap<String,Integer>();
		HMcj.put("mimmo",2);
		HMcj.put("francesco", 1);
		HMcj.put("dave", 1);
		HMcj.put("egidio", 3);
		
		double e = 0.0;
		int size = getTotalRecord(HMcj);
		System.out.println("size = "+size);
		for (Map.Entry<String, Integer> entry : HMcj.entrySet()) {
		      String cx = entry.getKey();
		      double p = (double) entry.getValue() / size;
		      e += p * log2(p);
		}
		e = -e;
		
		
		System.out.println("L'entropia è :"+e);
        */
		
		/*
		String cand1 = "0,3,4";
		String cand2 = "3,4";
		
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
		
		System.out.println(ret1.substring(0,ret1.length()-1));
		
		String ret2 = "";
		for(int i=0; i<cand1split.length; i++) {
			if(!cand1split[i].equals(""))
				ret2+=cand1split[i]+",";
		}
		System.out.println(ret2.substring(0,ret2.length()-1));
		*/
		
		System.out.println(minimalFD("3,4","0,3,4","4->0"));
		System.out.println(Utility.minimalFD("3,4", "0,3,4", "4->0"));
		
		/*
		 * 0,3,4 3,4
			0,3,4 0,4
			0,3,4 0,3
			0,2,3 2,3
			0,2,3 0,2
			0,2,3 0,3
		 */
		
		/*
		String[] a = new String[3];
		a[0] = "1";
		a[1] = "2";
		a[2] = "5";
		String[] b = new String[3];
		b[0] = "2";
		b[1] = "3";
		b[2] = "5";
		
		Set<String> S = new HashSet<String>();
		for(int i=0; i<a.length; i++) {
			S.add(a[i]);
		}
		
		for(int i=0; i<b.length; i++) {
			S.add(b[i]);
		}
		System.out.println(S.toString());
		
		String lll = "";
		Iterator it = S.iterator();
		while(it.hasNext()) {
			String tmp = (String) it.next();
			lll+=tmp+",";
		}
		System.out.println(lll);
		String[] SPLIT = lll.split(",");
		System.out.println("Size dello split "+SPLIT.length);
		*/
		
		MathContext context = new MathContext(100);
		
		BigDecimal value = new BigDecimal(125);
		BigDecimal count = new BigDecimal(4100);
		
		BigDecimal pro = value.divide(count,context);
		
		System.out.println(pro);
		
		
		
		
		String g = "0.03048780487804878048780487804878048780487804878048780487804878048780487804878048780487804878048780488";
		int c = 0;
		for(int i=0; i<g.length(); i++) {
			c++;
		}
		System.out.println(c);
		
		HashMap<String,BigDecimal> map = new HashMap<String,BigDecimal>();
		
		System.out.println("2,4,5");
		System.out.println("12.027772615618527");
		System.out.println("12.02777261561790406309485437347300399988732567498995862134691818929094090847577697323179036188428530");
		
		System.out.println("4,5,6");
		System.out.println("12.027772615618527");
		System.out.println("12.02777261561790406309485437347300399988732567498995862134691818929094090847577697323179036188428530");
		
		System.out.println("2,4,5,6");
		System.out.println("12.027772615618527");
		System.out.println("12.02777261561790406309485437347300399988732567498995862134691818929094090847577697323179036188428530");
		
		
		System.out.println("4,5,6,7");
		System.out.println("12.028251428163417");
		System.out.println("12.02825142816279273917816775628363363838385428403951571974289616391387602937594455762250139851344498");
		
		System.out.println("4,6,7");
		System.out.println("12.028251428163417");
		System.out.println("12.02825142816279273917816775628363363838385428403951571974289616391387602937594455762250139851344498");
		
		System.out.println("4,5,7");
		System.out.println("12.028251428163417");
		System.out.println("12.02825142816279273917816775628363363838385428403951571974289616391387602937594455762250139851344498");
		
		String key = "12.028251428162791";
	}

	private static double log2(double a) {
	    return Math.log(a) / Math.log(2);
	  }
	
	private static int getTotalRecord(Object2ObjectOpenHashMap<String,Integer> HMcj) {
		int count =0;
		for(int value: HMcj.values()) {
			count+=value;
		}
		return count;
	}
	
	private static boolean minimalFD(String LHS, String RHS, String FD) {
		
		String LHS_FD;
		String RHS_FD;
		
		LHS_FD = FD.substring(0,FD.indexOf("-")).trim();
		System.out.println("LHS di FD "+LHS_FD);
		
		RHS_FD = FD.substring(FD.indexOf("-")+2,FD.length()).trim();
		System.out.println("RHS di fd prima dell unione "+RHS_FD);
		
		
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
		System.out.println("RHS di fd buona "+RHS_FD);
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
		
		System.out.println(countLHS);
		
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
		
		System.out.println(countRHS);
		
		System.out.println("LHS diff "+differenceLHS);
		System.out.println("RHS diff "+differenceRHS);
		
		if(countLHS == LHS_FD_split.length && countRHS == RHS_FD_split.length && differenceLHS.equals(differenceRHS)) {
			return true;
		}else
			return false;
		
	}
	public static String[] unionArrays(String[]... arrays)
    {
        int maxSize = 0;
        int counter = 0;

        for(String[] array : arrays) maxSize += array.length;
        String[] accumulator = new String[maxSize];

        for(String[] array : arrays)
            for(String i : array)
                if(!isDuplicated(accumulator, counter, i))
                    accumulator[counter++] = i;

        String[] result = new String[counter];
        for(int i = 0; i < counter; i++) result[i] = accumulator[i];

        return result;
    }

    public static boolean isDuplicated(String[] array, int counter, String value)
    {
        for(int i = 0; i < counter; i++) if(array[i].equals(value)) return true;
        return false;
    }
	
    public static void searchEquivalentKey(ObjectArrayList<String> equivalent_key, 
			Object2ObjectOpenHashMap<String, Double> candidateLevelk,
			Object2ObjectOpenHashMap<String, Double> candidateLevelkminus1,
			int level) {
		
		Iterator it = candidateLevelk.entrySet().iterator();
		while(it.hasNext()) {
			
			Map.Entry<String, Double> pair = (Entry<String, Double>) it.next();
			String key = pair.getKey();
			Double value = pair.getValue();
			
			/*
			 * Generating the components of level k-1 from the candidate
			 * Similiar at generatingEdge
			 */
			
			
			Object2ObjectOpenHashMap<String, Double> component = new Object2ObjectOpenHashMap<String,Double>();
			String[] componentSupport = new String[level+1];
			int count=0;
			Iterator it2 = candidateLevelkminus1.entrySet().iterator();
			while(it2.hasNext() && count<(level+1)) {
				
				Map.Entry<String, Double> pair2 = (Entry<String,Double>) it2.next();
				String[] subkey = pair2.getKey().split(",");
				Double newvalue = pair2.getValue();
				
				
				
			}
			/*
			 * Search equivalent key
			 */
			
			int count_equivalent = 0;
			
			for(int i=0; i<componentSupport.length; i++) {
				if(componentSupport[i] != null) {
					if(component.get(componentSupport[i]).equals(value)) {
						count_equivalent++;
					}else
						componentSupport[i] = null;
				}
				
			}
			
			int toEliminate = count_equivalent-1;
			
			System.out.println(" For Candidate "+key+ " ----->  toEliminate = "+toEliminate+" \n");
			
			if(count_equivalent > 1) {
				for(int i=0; i<componentSupport.length; i++) {
					if(componentSupport[i] != null && toEliminate > 0) {
						System.out.println("Eliminating "+componentSupport[i]);
						equivalent_key.add(componentSupport[i]);
						toEliminate--;
					}
				}
			}
			System.out.println("  ------------------------------------------------------ ");
		}
    }
    
}
