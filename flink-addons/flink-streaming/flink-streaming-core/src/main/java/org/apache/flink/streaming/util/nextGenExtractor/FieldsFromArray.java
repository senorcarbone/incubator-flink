package org.apache.flink.streaming.util.nextGenExtractor;

import java.lang.reflect.Array;

import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class FieldsFromArray<OUT> implements NextGenExtractor<Object, OUT[]> {

	/**
	 * Auto-generated version id
	 */
	private static final long serialVersionUID = 8075055384516397670L;
	private int[] order;
	private Class<OUT> clazz;
	
	public FieldsFromArray(Class<OUT> clazz,int... indexes) {
		this.order=indexes;
		this.clazz=clazz;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public OUT[] extract(Object in) {
		OUT[] output=(OUT[])Array.newInstance(clazz, order.length);
		for (int i=0;i<order.length;i++){
			output[i]=(OUT)Array.get(in, this.order[i]);
		}
		return output;
	}

}
