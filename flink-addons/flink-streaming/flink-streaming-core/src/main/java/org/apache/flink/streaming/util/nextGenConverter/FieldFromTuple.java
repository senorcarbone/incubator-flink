package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.operator.NextGenTypeExtractor;

public class FieldFromTuple<OUT> implements NextGenTypeExtractor<Tuple, OUT> {

	/**
	 * Auto-gernated version id
	 */
	private static final long serialVersionUID = -5161386546695574359L;
	private int fieldId=0;
	
	public FieldFromTuple() {
		//noting to do => will use default 0
	}
	
	public FieldFromTuple(int fieldId) {
		this.fieldId=fieldId;
	}
	
	@Override
	public OUT convert(Tuple in) {
		return in.getField(fieldId);
	}

}
