package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.java.tuple.Tuple;

public class FieldsFromTuple implements NextGenExtractor<Tuple, double[]> {

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
