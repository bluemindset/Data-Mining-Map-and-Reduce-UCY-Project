package supermarket;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Simple Reducer gathers all the values and sums the supports
 * If the support sum is up to two then print it.
 */
public class ReducerPhase1  extends Reducer <Text, IntWritable, Text, IntWritable>{

	 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		        int sum = 0;
		        /*Gather support sum of current key */
		        for (IntWritable val : values) {
		        	sum+=val.get();
		        }
		        /*For support up to two print it*/
		        if (sum>=2)
		        context.write(key, new IntWritable(sum));
		    }
}
