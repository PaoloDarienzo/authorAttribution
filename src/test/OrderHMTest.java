package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

public class OrderHMTest {

	public static void main(String[] args) {
		
		HashMap<String, Float> results = new HashMap<String, Float>();
		
		results.put("First", (float) 0.14);
		results.put("Third", (float) 0.14);
		results.put("Second", (float) 2.11);
		
		System.out.println("results before: " + results.toString());
		System.out.println(orderHashMapByValueToString(results));
		System.out.println("results after: " + results.toString());
		
	}
	
	public static String orderHashMapByValueToString(HashMap<String, Float> results) {
		
		List<String> mapKeys = new ArrayList<>(results.keySet());
	    List<Float> mapValues = new ArrayList<>(results.values());
	    //Collections.sort(mapValues);
	    //Collections.sort(mapKeys);
	    Collections.sort(mapValues, Collections.reverseOrder());
	    Collections.sort(mapKeys, Collections.reverseOrder());

	    LinkedHashMap<String, Float> sortedMap = new LinkedHashMap<>();

	    Iterator<Float> valueIt = mapValues.iterator();
	    while (valueIt.hasNext()) {
	    	Float val = valueIt.next();
	        Iterator<String> keyIt = mapKeys.iterator();

	        while (keyIt.hasNext()) {
	        	String key = keyIt.next();
	        	Float comp1 = results.get(key);
	        	Float comp2 = val;

	            if (comp1.equals(comp2)) {
	                keyIt.remove();
	                sortedMap.put(key, val);
	                break;
	            }
	        }
	    }
	    //building the string result
	    //return sortedMap.toString();
	    
	    String result = "";
	    for(Entry<String, Float> entry : sortedMap.entrySet()) {
	    	result += entry.getKey() + ", " + entry.getValue() + "\n";
	    }
	    return result;
	    
	}
	
}
