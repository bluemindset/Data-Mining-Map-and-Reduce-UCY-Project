package supermarket;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;

public class ReduceOutputObjectPhase2 implements Writable{

	/*We want to gather all the output objects of the map and put them in a list 
	 * for each key.
	 */
	private HashSet <MapOutputObjectPhase2> tokens = new HashSet<MapOutputObjectPhase2>();
	
	public ReduceOutputObjectPhase2(){}
	
	public ReduceOutputObjectPhase2(Iterable<MapOutputObjectPhase2> objs){
		for(MapOutputObjectPhase2 obj :objs){
			tokens.add(new MapOutputObjectPhase2(obj));
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int lenght = in.readInt();
		
		tokens = new HashSet<MapOutputObjectPhase2>(lenght);
		
		for(int i = 0; i<lenght; i++){
			MapOutputObjectPhase2 index = new MapOutputObjectPhase2();
			index.readFields(in);
			tokens.add(index);
			
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(tokens.size());
		for (MapOutputObjectPhase2 obj : tokens) {
			obj.write(out);
		}
	}

}
