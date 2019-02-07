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
		        float confidence = 0f;
		        int supportdown = 0;
		        int supportup =0;
		        String rule = null;
		        String str =null;
		        for(Text s : values){
				      if (!s.toString().contains("->")){

		        	  // System.out.println("\n\nKey is :"+key.toString() +" Value is " +s.toString());
		        	 str = s.toString();
		        	if (str.contains("0")){
		        		str = str.replace("0","");
		        		str = str.replace(",","");
		        		str = str.replace(":","");
		        		str=  str.replace(" ","");

				        supportdown = Integer.parseInt(str);
				      //  System.out.println("\n\nZERO UP " +key+"  "+ supportdown);
		        	}
		        	else{
				        StringTokenizer tokensemi= new StringTokenizer(str,":");
				        rule = tokensemi.nextToken();
				        supportup = Integer.parseInt(tokensemi.nextToken());
				     //   System.out.println("\n\nZERO Down " +key+"  "+ supportup);
		        		}
				      }
		        }
		        if (rule!=null  ){
				        confidence = supportup/supportdown;
				        rule = rule.replace("]", "");
				        rule = rule.replace("[", "");
				        String k = key.toString();
				        k = k.replace("]", "");
				        k= k.replace("[", "");
				        StringTokenizer tokenizer1 = new StringTokenizer(rule,",");	  
				        StringTokenizer tokenizer2 = new StringTokenizer(k.toString(),",");
				        
				        ArrayList<String> tokens1 = new ArrayList<String>(); 
				        ArrayList<String> tokens2= new ArrayList<String>(); 

				        while (tokenizer1.hasMoreTokens()) {
				        	tokens1.add(tokenizer1.nextToken());
				        } 	
				        while (tokenizer2.hasMoreTokens()) {
				        	tokens2.add(tokenizer2.nextToken());
				        } 	
				        
				        String val = null;
				        if(tokens1.size()>tokens2.size()){
					        for (String t :tokens1){
					        	if (tokens2.contains(t))
					        		val = t;
					        }
					       
					        StringBuilder sf = new StringBuilder();
					        sf.append(" -> " + val);
					        String number = Float.toString(confidence);
					        Text sf1 = new Text(sf.toString()) ;
					        System.out.println("\n\nKey" +tokens1.toString());
					        System.out.println("\n\nRule " +tokens2.toString());
					        context.write(key,sf1);
				        }
		        }
		   }
	 }


