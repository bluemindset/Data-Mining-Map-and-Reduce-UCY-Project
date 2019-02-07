package supermarket;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import supermarket.MapperPhase1;
import supermarket.ReducerPhase1;



public class Indexer2 {

	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf2 = new Configuration();

		Job job2 = Job.getInstance(conf2, "Phase 2");
	    job2.setJarByClass(Indexer2.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);

	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputKeyClass( Text.class);
	    job2.setOutputValueClass(Text.class);
		
	    job2.setMapperClass(MapperPhase2.class);
	    job2.setCombinerClass(ReducerPhase2.class);
	    job2.setReducerClass(ReducerPhase2.class);
	    
	    
		FileInputFormat.addInputPath(job2, new Path(
				"hdfs://localhost:54310/user/csdeptucy/output/phase1"));
		FileOutputFormat.setOutputPath(job2, new Path(
				"hdfs://localhost:54310/user/csdeptucy/output/phase2"));
	 	
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

