package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class FieldFromArray<OUT> implements NextGenExtractor<Object[], OUT> {

	/**
	 * Auto-gernated version id
	 */
	private static final long serialVersionUID = -5161386546695574359L;
	private int fieldId=0;
	
	public FieldFromArray() {
		//noting to do => will use default 0
	}
	
	public FieldFromArray(int fieldId) {
		this.fieldId=fieldId;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public OUT convert(Object[] in) {
		return (OUT)in[fieldId];
	}
	
}
