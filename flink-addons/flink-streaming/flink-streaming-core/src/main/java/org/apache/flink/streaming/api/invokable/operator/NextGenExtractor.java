package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;

public interface NextGenExtractor<FROM,TO> extends Serializable{

	public TO convert(FROM in);
	
}
