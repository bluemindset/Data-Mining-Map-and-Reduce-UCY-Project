package supermarket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*This reducer for each key must collect
 * A . All bigger rules with support
 * B . The current support of the key which is 0:support 
 * To manage this we must gather the 2 values of the key first and then compute
 * the confidence. A key also might have more than two values but with different rules.
 */

public class ReducerPhase2  extends Reducer <Text,Text, Text, Text>{

	 public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		        String str = null;
		        String rule = null;
		        float currentSupport = 0;
		        int supportup;
		        String val = null;
		        String keyS = null;
		        float confidence = 0;
		        ArrayList<String> rules = new ArrayList<String>();
		        ArrayList<Integer> supports = new ArrayList<Integer>();
		        
		        for(Text s : values){
		        	/*For each value if it is not contains 0 
		        	 * Then gather the rules and store them to compute confidence*/
		        	 str = s.toString();
				     if (!str.contains("0")){
				     		 StringTokenizer tokensemi= new StringTokenizer(str,":");
						     rule = tokensemi.nextToken();
						     supportup = Integer.parseInt(tokensemi.nextToken());
						     /* Tokenize the rule and the key  and check what element it has more 
						      * so you can extract it and print on -> 
						      */
						     StringTokenizer tokenizer1 = new StringTokenizer(rule,",");
						     keyS = key.toString().replace("[","");
						     keyS = keyS.replace("]","");
						     StringTokenizer tokenizer2 = new StringTokenizer(keyS,",");
						     ArrayList<String> tokens1 = new ArrayList<String>(); 
						     ArrayList<String> tokens2= new ArrayList<String>(); 

						     while (tokenizer1.hasMoreTokens()) {
						    	 String sa = tokenizer1.nextToken();
						        tokens1.add(sa);
						     } 	
						     while (tokenizer2.hasMoreTokens()) {
						        tokens2.add(tokenizer2.nextToken());
						     } 	
						     boolean add=true ;
							        for (String t :tokens1){
							        	String k = t.replace(" ","");
							        	add =true;
							        	for(int i =0 ; i<tokens2.size();i++){
							        		if (tokens2.get(i).equals(k))
							        			add = false;
							        	}
							        	if (add)
							        		 rules.add(t);
							        }
							       
							        supports.add(supportup);   
				     	}
				     	else{
				     		/*Else if it contains 0 get the support of the rule*/
				     		str = str.replace("0","");
			        		str = str.replace(",","");
			        		str = str.replace(":","");
			        		str=  str.replace(" ","");
			        		currentSupport= Float.parseFloat(str);				     		
				     	}
				
				     if (!supports.isEmpty()&&currentSupport!= 0 ){
				    	 /*Calculate the confidence for each rule and write it to context*/
				    	for(int i =0; i <supports.size();i++){
					        confidence = supports.get(i).intValue()/currentSupport;				     
					        context.write(key,new Text(" -> "+rules.get(i)+"  "+String.valueOf(confidence)));
					       
				    	 }
				    	 rules.clear();
				        	supports.clear();
				        	val = null;
				     }
				     
		        }	     
	 	}
}


