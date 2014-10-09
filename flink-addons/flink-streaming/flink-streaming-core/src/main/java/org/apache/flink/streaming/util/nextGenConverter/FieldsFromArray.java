package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.streaming.api.invokable.operator.NextGenTypeExtractor;

public class FieldsFromArray implements NextGenTypeExtractor<Object[], double[]> {

	/**
	 * Auto-generated version id
	 */
	private static final long serialVersionUID = 8075055384516397670L;
	int[] order;
	
	public FieldsFromArray(int... indexes) {
		this.order=indexes;
	}
	
	@Override
	public double[] convert(Object[] in) {
		double[] output=new double[order.length];
		for (int i=0;i<order.length;i++){
			output[i]=(Double) in[order[i]];
		}
		return output;
	}

}
