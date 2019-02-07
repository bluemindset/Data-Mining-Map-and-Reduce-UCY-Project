package supermarket;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;



public  class MapOutputObjectPhase2 implements Writable{
	
	private IntWritable index;
	
	public MapOutputObjectPhase2(int index){
		
		this.index = new IntWritable(index);
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
	
		//tokens.out(out)
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	

}
