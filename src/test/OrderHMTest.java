package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import support.MethodsCollection;

public class OrderHMTest {

	public static void main(String[] args) {
		
		HashMap<String, Float> results = new HashMap<String, Float>();
		
		results.put("First", (float) 0.14);
		results.put("Third", (float) 0.14);
		results.put("Second", (float) 2.11);
		results.put("A", (float) 10);
		results.put("B", (float) 9);
		results.put("C", (float) 7);
		results.put("D", (float) 6);
		results.put("E", (float) 2);
		results.put("F", (float) 1);
		results.put("G", (float) 6);
		results.put("H", (float) 2.6);
		results.put("I", (float) 1);
		
		//System.out.println("results before: " + results.toString());
		System.out.println(orderHashMapByValueToString(results));
		//System.out.println("results after: " + results.toString());
		System.out.println(extractSubSetOrdered(results));
		
		System.out.println();
		System.out.println(MethodsCollection.entriesSortedByValues(results));
		
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
	
	public static LinkedHashMap<String, Float> orderHashMapByValue(HashMap<String, Float> results) {
		
		List<String> mapKeys = new ArrayList<>(results.keySet());
	    List<Float> mapValues = new ArrayList<>(results.values());
	    Collections.sort(mapValues, Collections.reverseOrder());
	    Collections.sort(mapKeys, Collections.reverseOrder());
	    //for ascending order
	    //Collections.sort(mapValues);
	    //Collections.sort(mapKeys);

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
	    
	    return sortedMap;
	    
	}

	public static LinkedHashMap<String, Float> extractSubSetOrdered(HashMap<String, Float> hashMap){
		//extracting 2nd-6th entries from ordered set
		if(hashMap.size() < 6) {
			return null;
		}
		else {
			LinkedHashMap<String, Float> orderedMap = orderHashMapByValue(hashMap);
			LinkedHashMap<String, Float> subSetOdered = new LinkedHashMap<String, Float>();
			int counter = 0;
			for(Entry<String, Float> entry : orderedMap.entrySet()) {
				counter++;
				if(counter<2) {
					continue;
				}
				else if(counter > 6) {
					break;
				}
				else {
					subSetOdered.put(entry.getKey(), entry.getValue());
				}
			}
			return subSetOdered;
		}
	}
	
}
