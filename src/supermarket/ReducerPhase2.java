package supermarket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase2  extends Reducer <Text,MapOutputObjectPhase2, Text, FloatWritable>{

	 public void reduce(Text key, Iterable<MapOutputObjectPhase2> values, Context context) 
		      throws IOException, InterruptedException {
		        String keyoutput = new String();
		 		/*Get the key*/
		        float confidence = 0;
		        int supportdown = 0;
		        int supportup =0;
		        for ( MapOutputObjectPhase2 val : values) {
		        	if (val.getRule().toString().equals("0")){
		        		supportup = val.getSupport().get();
		        		keyoutput = val.getRule().toString();
		        	}
		        	else{
		        		supportdown = val.getSupport().get();
		        	}
		        }
		        confidence = supportup/supportdown;

		        
		        String line1  = key.toString();
		        String line2 = keyoutput.toString();
		 
		        StringTokenizer tokenizer1 = new StringTokenizer(line1,",");	  
		        StringTokenizer tokenizer2 = new StringTokenizer(line2,",");
		 
		        ArrayList<String> tokens1 = new ArrayList<String>(); 
		        ArrayList<String> tokens2= new ArrayList<String>(); 
	 	        
	 	       
		        while (tokenizer1.hasMoreTokens()) {
		        	tokens1.add(tokenizer1.nextToken());
		        } 	
		        while (tokenizer2.hasMoreTokens()) {
		        	tokens2.add(tokenizer2.nextToken());
		        } 	
		        String val = null;
		        for (String t :tokens2){
		        	if (tokens1.contains(t))
		        		val = t;
		        }
		        StringBuilder s = new StringBuilder();
		        s.append(tokens2.toString() +" -> " + val);
		        context.write(new Text(s.toString()),new FloatWritable(confidence));
		    }
}
