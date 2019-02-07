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
	
	public IntWritable getSupport() {
		return support;
	}

	public void setSupport(IntWritable support) {
		this.support = support;
	}

	public Text getRule() {
		return rule;
	}

	public void setRule(Text rule) {
		this.rule = rule;
	}
	
	public MapOutputObjectPhase2(Text rule,int index){
		this.rule = rule;
		this.support = new IntWritable(index);
	}
	
	/*Default constructor for read fields*/
	public MapOutputObjectPhase2(){
		this.support = new IntWritable();
		this.rule = new Text();
	}
	public MapOutputObjectPhase2(MapOutputObjectPhase2 obj){
		this.support = obj.getSupport();
		this.rule = obj.getRule();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
	
		rule.readFields(in);
		support.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.rule = new Text();
		this.support = new IntWritable();
		rule.write(out);
		support.write(out);
			
	}
	
	

}
