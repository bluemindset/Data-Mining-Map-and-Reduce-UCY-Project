package supermarket;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;



public  class MapOutputObjectPhase2 implements Writable{
	
	private IntWritable support;
	private Text rule;
	public MapOutputObjectPhase2(Text rule,int index){
		this.rule = rule;
		this.support = new IntWritable(index);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	
		rule.readFields(in);
		support.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		rule.write(out);
		support.write(out);
			
	}
	
	

}
