package supermarket;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import supermarket.MapperPhase1;
import supermarket.ReducerPhase1;



public class Indexer {

	public void phase1(Configuration conf) throws IOException{

	    Job job = Job.getInstance(conf, "Phase 1");
	    job.setJarByClass(Indexer.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputKeyClass( Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setMapperClass(MapperPhase1.class);
	    job.setCombinerClass(ReducerPhase1.class);
	    job.setReducerClass(ReducerPhase1.class);
		
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://localhost:54310/user/csdeptucy/input/supermarket"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://localhost:54310/user/csdeptucy/output/supermarket"));
	}

	public void phase2(){
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        
		
		
		phase1(conf);
		
		/******End of first phase********/
		
		phase2(conf);
		
		


	

	   // 
		

		/******END OF FIRST PHASE*******/
		
		
	}
}

