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

	    job.setMapperClass(MapperPhase1.class);
	    job.setCombinerClass(ReducerPhase1.class);
	    job.setReducerClass(ReducerPhase1.class);
		
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://localhost:54310/user/csdeptucy/input/supermarket"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://localhost:54310/user/csdeptucy/output/phase1"));
	
		job.waitForCompletion(true);

	}

	public void phase2() throws ClassNotFoundException, IOException, InterruptedException {
			Configuration conf2 = new Configuration();

			Job job2 = Job.getInstance(conf2, "Phase 2");
		    job2.setJarByClass(Indexer2.class);
		    job2.setOutputFormatClass(TextOutputFormat.class);

		    job2.setInputFormatClass(TextInputFormat.class);
		    job2.setOutputKeyClass( Text.class);
		    job2.setOutputValueClass(Text.class);
			
		    job2.setMapperClass(MapperPhase2.class);
		    job2.setReducerClass(ReducerPhase2.class);
		    
		    
			FileInputFormat.addInputPath(job2, new Path(
					"hdfs://localhost:54310/user/csdeptucy/output/phase1"));
			FileOutputFormat.setOutputPath(job2, new Path(
					"hdfs://localhost:54310/user/csdeptucy/output/phase2"));
		 	
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	
	public void phase3() throws IOException{
		
		 BufferedReader br = new BufferedReader(new FileReader("hdfs://localhost:54310/user/csdeptucy/output/phase2/part-r-00000");
"));
		 String line = null;
		 while ((line = br.readLine()) != null) {
		   System.out.println(line);
		 }
		
		
	}
	
	public static void main(String[] args) throws Exception {
		Indexer i =  new Indexer();
		i.phase1();
		i.phase2();
		i.phase3();
	}

	
	
	
	
	
}

