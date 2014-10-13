package org.apache.flink.streaming.util.nextGenExtractor;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class FieldsFromTuple implements NextGenExtractor<Tuple, double[]> {

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -2554079091050273761L;
	int[] indexes;

	public FieldsFromTuple(int... indexes) {
		this.indexes = indexes;
	}

	@Override
	public double[] extract(Tuple in) {
		double[] out = new double[indexes.length];
		for (int i = 0; i < indexes.length; i++) {
			out[i] = in.getField(indexes[i]);
		}
		return out;
	}
}
