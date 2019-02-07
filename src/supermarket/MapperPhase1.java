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


public class MapperPhase1  extends Mapper<LongWritable, Text,Text,IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
   
    private FindCombinations  combClass = new FindCombinations();
	private ArrayList<ArrayList<String>> combs = new ArrayList<ArrayList<String>>();

	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	      
	    	String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,",");	        
	        ArrayList<String> tokens = new ArrayList<String>(); 
 	        
 	       
	        while (tokenizer.hasMoreTokens()) {
	        	tokens.add(tokenizer.nextToken());
	        } 	
	     //   System.out.print(tokens);
	        combs =  combClass.exploitCombinations(tokens);
        	System.out.println(combs);

	        for(ArrayList<String> comb :combs ){	        	
	        	Text keyText = new Text(comb.toString());
	       	   	context.write(keyText,one);	 
	        }
	        combs.clear();
	 }
}

	        	      
	 
	
