package supermarket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase2  extends Reducer <Text,Text, Text, Text>{

	 public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		 		/*Get the key*/
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
		        	
		        	 str = s.toString();
				     if (!str.contains("0")){
				    	
				     		 StringTokenizer tokensemi= new StringTokenizer(str,":");
						     rule = tokensemi.nextToken();
						     supportup = Integer.parseInt(tokensemi.nextToken());

						     StringTokenizer tokenizer1 = new StringTokenizer(rule,",");
						     keyS = key.toString().replace("[","");
						     keyS = keyS.replace("]","");
						     StringTokenizer tokenizer2 = new StringTokenizer(keyS,",");
						     ArrayList<String> tokens1 = new ArrayList<String>(); 
						     ArrayList<String> tokens2= new ArrayList<String>(); 

						     while (tokenizer1.hasMoreTokens()) {
						    	 String sa = tokenizer1.nextToken();
						    	 System.out.println(sa);
						        tokens1.add(sa);
						     } 	
						     while (tokenizer2.hasMoreTokens()) {
						        tokens2.add(tokenizer2.nextToken());
						     } 	
						    
						    	 
						     
						     System.out.println(tokens1.size());
						     System.out.println(tokens2.size());
						     boolean add=true ;
							        for (String t :tokens1){
							        	String k = t.replace(" ","");
							        	add =true;
							        	for(int i =0 ; i<tokens2.size();i++){
							        		System.out.println(k+tokens2.get(i));
							        		if (tokens2.get(i).equals(k))
							        			add = false;
							        	}
							        	if (add)
							        		 rules.add(t);
							        }
							       
							        supports.add(supportup);   
				     	}
				     	else{
				     		str = str.replace("0","");
			        		str = str.replace(",","");
			        		str = str.replace(":","");
			        		str=  str.replace(" ","");
			        		currentSupport= Float.parseFloat(str);				     		
				     	}
				
				     if (!supports.isEmpty()&&currentSupport!= 0 ){
				    	for(int i =0; i <supports.size();i++){
					        confidence = supports.get(i).intValue()/currentSupport;				     
					        context.write(key,new Text("->"+rules.get(i)+"  "+String.valueOf(confidence)));
					       
				    	 }
				    	 rules.clear();
				        	supports.clear();
				        	val = null;
				     }
				     
		        }	     
	 	}
}


