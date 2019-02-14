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
         {   /*Vres to kainourgio sindiasmo kai kane ton add mesa stin lista*/
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
        /* With recursion find all the possible combinations of the rules
         * each time for ones,twos , threes and so on.Recursivle build tree from one 
         * side to another*/
        phase1Combinations(arr, size, r, index+1, copy, i+1); 
        phase1Combinations(arr, size, r, index, copy, i+1); 
    }
    /*Convert the array list string to array*/
    public static String[] GetStringArray(ArrayList<String> arr) 
    { 
        String strarr[] = new String[arr.size()]; 
 
        for (int j = 0; j < arr.size(); j++) { 
              strarr[j] = arr.get(j); 
        } 
  
        return strarr; 
    } 
    /*Find all the possible combinations*/
    public ArrayList<ArrayList<String>> exploitCombinations(ArrayList<String> fresh){
    	String arr[]= GetStringArray(fresh);
    	combs.clear(); 
    	for(int i = 1; i <=arr.length ;i++ ){
             String data[]=new String[i]; 
             phase1Combinations(arr, arr.length, i, 0, data, 0); 
         }
    	 return combs;
    }
    /*Find all the rules by each time removing one element and put the new array list 
     * into an array list of arraylists.
     */
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