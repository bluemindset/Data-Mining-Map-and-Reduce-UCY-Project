package supermarket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Mapper phase 2 gets the line from the phase 1 input file and 
 * for each value  is exports the values  0:support (its support)
 * the support for the other rule  and as key the value that it got.
 */
public class MapperPhase2  extends Mapper<LongWritable,Text,Text,Text> {

     private FindCombinations  combClass = new FindCombinations();
	 private ArrayList<ArrayList<String>> combs = new ArrayList<ArrayList<String>>();

	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    /*Get the line from the text */
		 	String line = value.toString();
		 	/* Strip it to the first token which is the array*/
	        StringTokenizer tokenizer1 = new StringTokenizer(line,"\t");	        
	        String combination = tokenizer1.nextToken();
        	ArrayList<String> tokens = new ArrayList<String>(); 

	        int support=  Integer.parseInt(tokenizer1.nextToken());
	        /*Clean the string*/
	        combination =  combination.replace(" ","");
	        combination =  combination.replace("]","");
	        combination =  combination.replace("[","");
	        	
	        /*Create all the rules from the array*/	        
	        if (combination.contains(",")){
	        	StringTokenizer tokenizer2 = new StringTokenizer(combination,",");	
	        	
	        	while (tokenizer2.hasMoreTokens()) {
	        		tokens.add(tokenizer2.nextToken());
	        	} 
	        	/*Produce all the rules from the value 
	        	 * Clean the string from spaces and export it
	        	 * key : new rule combination found 
	        	 * value : current rule combination and support of the rule  */
		        combs =  combClass.ruleCombinations(tokens);
		        while (!combs.isEmpty()){
			    	StringBuilder val = new StringBuilder();
		        	Text valText = new Text(combination);
		        	String keyT = combs.get(0).toString();
		        	keyT = keyT.replace(" ", "");
		        	Text keyText = new Text(keyT);
		        	val.append(valText+":"+support);
		        	combs.remove(0);
		        	context.write(keyText,new Text(val.toString()));
		        }
	        }

	          /* Write the current combination of the basket
	         *   Give its support and put 0 in the value so we can 
	         *   later recognize it inside reducer.
	          */
	        StringBuilder s = new StringBuilder();
	        s.append(" 0 :"+support);
	        StringBuilder k = new StringBuilder();
	        k.append("["+combination+"]");
	        Text keyText = new Text(k.toString());
	        context.write(keyText,new Text(s.toString()));

	        combs.clear();
	 }
}

	        	      
	 
	
