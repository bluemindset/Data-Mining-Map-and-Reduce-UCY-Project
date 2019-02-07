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


public class MapperPhase2  extends Mapper<LongWritable,Text,Text,MapOutputObjectPhase2> {

     private FindCombinations  combClass = new FindCombinations();
	 private ArrayList<ArrayList<String>> combs = new ArrayList<ArrayList<String>>();

	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    /*Get the line from the text */
		 	String line = value.toString();
		 	/* Strip it to the first token which is the array*/
	        StringTokenizer tokenizer1 = new StringTokenizer(line,"/t");	        
	        String combination = tokenizer1.nextToken();
	        System.out.println(combination);
	        int support = Integer.parseInt(tokenizer1.nextToken());
	        /*Clean the string*/
	        combination.replaceAll(" ","");
	        combination.replaceAll("]","");
	        combination.replaceAll("[","");
	        /*Create all the rule from the array*/	        
	        StringTokenizer tokenizer2 = new StringTokenizer(combination,",");	        
	    	ArrayList<String> tokens = new ArrayList<String>(); 

	        while (tokenizer2.hasMoreTokens()) {
	        	tokens.add(tokenizer2.nextToken());
	        } 
	        combs =  combClass.ruleCombinations(tokens);
	        while (!combs.isEmpty()){
	        	Text valText = new Text(combination);
	        	Text keyText = new Text(combs.get(0).toString());
	        	MapOutputObjectPhase2 val = new MapOutputObjectPhase2(valText,support);
	        	combs.remove(0);
	        	context.write(keyText,val);
	        }
	        
	        Text keyText = new Text(combination);
	        Text valText = new Text("0");
	        /*
	         * Write the final combination of the basket
	         * Give it support 0
	         * */
	        MapOutputObjectPhase2 val = new MapOutputObjectPhase2(valText,support);
	        context.write(keyText,val);
	     
	        combs.clear();
	 }
}

	        	      
	 
	
