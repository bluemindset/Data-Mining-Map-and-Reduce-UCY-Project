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
 * Mapper for phase 1. Gets one line of the input file(one basket) ,
 * and finds all the combinations for that basket as shown in the diagram using
 * function FindCombinations.exploitCombinations()
 * 
 */
public class MapperPhase1  extends Mapper<LongWritable, Text,Text,IntWritable> {

    private final static IntWritable one = new IntWritable(1);
   
    private FindCombinations  combClass = new FindCombinations();
	private ArrayList<ArrayList<String>> combs = new ArrayList<ArrayList<String>>();

	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	      	/*Tokenize the line*/
	    	String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,",");	        
	        ArrayList<String> tokens = new ArrayList<String>(); 
 	        
	        while (tokenizer.hasMoreTokens()) {
	        	tokens.add(tokenizer.nextToken());
	        } 	
	        /*Get all the combinations and write them in the context as key and 1 */
	        combs =  combClass.exploitCombinations(tokens);

	        for(ArrayList<String> comb :combs ){	        	
	        	Text keyText = new Text(comb.toString());
	        	/*for each combination found write is a key and value 1*/
	       	   	context.write(keyText,one);	 
	        }
	        combs.clear();
	 }
}

	        	      
	 
	
