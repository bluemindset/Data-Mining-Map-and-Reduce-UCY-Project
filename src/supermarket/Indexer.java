package supermarket;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

	public void phase1() throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Phase 1");

	    job.setJarByClass(Indexer.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputKeyClass( Text.class);
	    job.setOutputValueClass(IntWritable.class);
		/*Different classes assigned for phase 1*/

	    job.setMapperClass(MapperPhase1.class);
	    job.setCombinerClass(ReducerPhase1.class);
	    job.setReducerClass(ReducerPhase1.class);
	    
	    /*Different paths added for phase 1*/
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://localhost:54310/user/csdeptucy/input/input_945470"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://localhost:54310/user/csdeptucy/output/phase1_945470"));
	
		job.waitForCompletion(true);

	}

	public void phase2() throws ClassNotFoundException, IOException, InterruptedException {
			Configuration conf2 = new Configuration();

			Job job2 = Job.getInstance(conf2, "Phase 2");
		    job2.setJarByClass(Indexer.class);
		    job2.setOutputFormatClass(TextOutputFormat.class);

		    job2.setInputFormatClass(TextInputFormat.class);
		    job2.setOutputKeyClass( Text.class);
		    job2.setOutputValueClass(Text.class);
			/*Different classes assigned for phase 2*/
		    job2.setMapperClass(MapperPhase2.class);
		    job2.setReducerClass(ReducerPhase2.class);
		    
		    /*Different paths added for phase 2*/
			FileInputFormat.addInputPath(job2, new Path(
					"hdfs://localhost:54310/user/csdeptucy/output/phase1_945470"));
			FileOutputFormat.setOutputPath(job2, new Path(
					"hdfs://localhost:54310/user/csdeptucy/output/phase2_945470"));
		 	
			job2.waitForCompletion(true);
		}
	

	
	public static void main(String[] args) throws Exception {
		
		Indexer i =  new Indexer();
		/*Run the first phase */
		i.phase1();
		/*Run the second phase */
		i.phase2();
	}

	
	
	
	
	
}

