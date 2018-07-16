package utilities;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;


public class Utility {

	public static Object2ObjectOpenHashMap<String,ObjectArrayList<Attribute>> generateCandidateList(String list) {  
		
		Object2ObjectOpenHashMap<String,ObjectArrayList<Attribute>> mapLevel = new Object2ObjectOpenHashMap<String,ObjectArrayList<Attribute>>();; //level{("1","list"),("n-1","list")}
		String[] split = list.split(",");  
		int numAttribute = split.length;
		
		//level1
		ObjectArrayList<Attribute> listLevelTemp = new ObjectArrayList<Attribute>();
		for(int i=0; i<numAttribute; i++) {
			Attribute tmp = new Attribute(split[i]);
			listLevelTemp.add(tmp);
			mapLevel.put("1", listLevelTemp);   // level1, list candidates
		}
		//In this level only attribute single..... A B C D
		
		//generate all level and take just 1 2 n n-1
		listLevelTemp = generateSubSet(split);
		mapLevel.put("2", takeLevel(listLevelTemp,2)); //level2, list candidates
		mapLevel.put("n-1", takeLevel(listLevelTemp,numAttribute-1)); //level2, list candidates
		mapLevel.put("n", takeLevel(listLevelTemp,numAttribute)); //level2, list candidates
		
		return mapLevel;
		
	}
	
	public static ObjectArrayList<Attribute> generateSubSet(String set[]) { //sembra funzionare bene per ora
		ObjectArrayList<Attribute> result = new ObjectArrayList<Attribute>();
        int n = set.length;
 
        for (int i = 0; i < (1<<n); i++)
        {
        	Attribute tmp = new Attribute();
            for (int j = 0; j < n; j++)
 
                if ((i & (1 << j)) > 0) {
                    
                    tmp.addAttribute(set[j]);
                }
            result.add(tmp);
        }
        
        return result;
    }
	
	public static ObjectArrayList<Attribute> takeLevel(ObjectArrayList<Attribute> set, int number){
		ObjectArrayList<Attribute> ret = new ObjectArrayList<Attribute>();
		
		for(int i=0; i<set.size(); i++) {
			Attribute tmp = set.get(i);
			if(tmp.getValues().size() == number)
				ret.add(tmp);
		}
		return ret;
	}
	
	
}
