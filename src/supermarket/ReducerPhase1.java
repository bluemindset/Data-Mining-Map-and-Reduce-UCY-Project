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

public class ReducerPhase1  extends Reducer <Text, IntWritable, Text, IntWritable>{

	 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		        int sum = 0;
		        for (IntWritable val : values) {
		        	sum+=val.get();
		        }
		        if (sum>=2)
		        context.write(key, new IntWritable(sum));
		    }
}
