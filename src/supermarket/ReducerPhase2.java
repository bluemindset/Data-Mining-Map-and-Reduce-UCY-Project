package supermarket;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase2  extends Reducer <Text,MapOutputObjectPhase2, Text, ReduceOutputObjectPhase2>{

	 public void reduce(Text key, Iterable<MapOutputObjectPhase2> values, Context context) 
		      throws IOException, InterruptedException {
		        int sum = 0;
		        for ( MapOutputObjectPhase2 val : values) {
		        	sum+=val.getSupport().get();
		        }
		        context.write(key, new ReduceOutputObjectPhase2(sum,key));
		    }
}
