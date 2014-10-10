package org.apache.flink.streaming.util.nextGenExtractor;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class ArrayFromTuple implements NextGenExtractor<Tuple, Object[]> {

	/**
	 * Auto generated version id
	 */
	private static final long serialVersionUID = -6076121226427616818L;
	int[] order=null;
	
	public ArrayFromTuple() {
		// noting to do
	}
	
	public ArrayFromTuple(int... indexes) {
		this.order=indexes;
	}
		
	@Override
	public Object[] extract(Tuple in) {
		Object[] output;
		
		if (order==null){
			//copy the hole tuple
			output=new Object[in.getArity()];
			for (int i=0;i<in.getArity();i++){
				output[i]=in.getField(i);
			}
		} else {
			//copy user specified order
			output=new Object[order.length];
			for (int i=0;i<order.length;i++){
				output[i]=in.getField(order[i]);
			}
		}
		
		return output;
	}

}
