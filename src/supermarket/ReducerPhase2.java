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
		        String rule = "rule";
		        int currentSupport;
		        int supportup;
		        String val = null;
		        String keyS = null;
		        
		        for(Text s : values){
		        	 str = s.toString();
		        	 System.out.println(str);
				     if (!str.contains("0")){
				     		 StringTokenizer tokensemi= new StringTokenizer(str,":");
						     rule = tokensemi.nextToken();
						     supportup = Integer.parseInt(tokensemi.nextToken());
						     StringTokenizer tokenizer1 = new StringTokenizer(rule,",");
						     keyS = key.toString().replace("[","");
						     keyS = keyS.replace("]","");
						     StringTokenizer tokenizer2 = new StringTokenizer(keyS);
						     ArrayList<String> tokens1 = new ArrayList<String>(); 
						     ArrayList<String> tokens2= new ArrayList<String>(); 

						     while (tokenizer1.hasMoreTokens()) {
						        tokens1.add(tokenizer1.nextToken());
						     } 	
						     while (tokenizer2.hasMoreTokens()) {
						        tokens2.add(tokenizer2.nextToken());
						     } 	
						        System.out.println(tokens1);
						        System.out.println(tokens2);
							        for (String t :tokens1){
							        	if (!tokens2.contains(t))
							        		val = t;
							        }
							        context.write(key,new Text("->"+val));
				     	}
				     	else{
				     		str = str.replace("0","");
			        		str = str.replace(",","");
			        		str = str.replace(":","");
			        		str=  str.replace(" ","");
			        		currentSupport= Integer.parseInt(str);				     		
				     	}
		        }	     
	 	}
}


