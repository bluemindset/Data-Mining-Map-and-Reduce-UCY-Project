package supermarket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

class FindCombinations { 
	public static ArrayList<ArrayList<String>> combs = new ArrayList<ArrayList<String>>();
    static void phase1Combinations(String arr[], int size, int r, int index, 
                                String copy[], int i) 
    { 	
    	 if (index == r) 
         {   
    		 ArrayList<String> generate = new ArrayList<String>();
    		 for (int j=0; j<r; j++) {
    			 generate.add(copy[j]);
    		 }
    	/*Sortare to kenairougio combination,dioti meta tha exi diaforetiko sindiasmo*/
    		 Collections.sort(generate, new Comparator<String>() {
    			    @Override
    			    public int compare(String s1, String s2) {
    			        return s1.compareToIgnoreCase(s2);
    			    }
    			});
            combs.add(generate);
    		 return; 
         } 
        if (i >= size) 
        return; 
        
        copy[index] = arr[i]; 
        phase1Combinations(arr, size, r, index+1, copy, i+1); 
        phase1Combinations(arr, size, r, index, copy, i+1); 
    } 
    public static String[] GetStringArray(ArrayList<String> arr) 
    { 
  
        // declaration and initialise String Array 
        String str[] = new String[arr.size()]; 
  
        // ArrayList to Array Conversion 
        for (int j = 0; j < arr.size(); j++) { 
  
            // Assign each value to String array 
            str[j] = arr.get(j); 
        } 
  
        return str; 
    } 
    public ArrayList<ArrayList<String>> exploitCombinations(ArrayList<String> fresh){
    	String arr[]= GetStringArray(fresh);
    	combs.clear(); 
    	for(int i = 1; i <=arr.length ;i++ ){
             String data[]=new String[i]; 
             phase1Combinations(arr, arr.length, i, 0, data, 0); 
         }

    	 return combs;
    }
    public ArrayList<ArrayList<String>> ruleCombinations(ArrayList<String> fresh){
    	combs.clear(); 
    	for(int i = 0 ; i<fresh.size();i++){
    		ArrayList<String> temp = new ArrayList<String>(fresh);
    		temp.remove(i);
    		combs.add(temp);
    	}
    	return combs;
    	
    }
} 