package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class ConcatinatedExtract<FROM,OVER,TO> implements NextGenExtractor<FROM, TO>{

	/**
	 * auto-generated id
	 */
	private static final long serialVersionUID = -7807197760725651752L;

	private NextGenExtractor<FROM, OVER> e1;
	private NextGenExtractor<OVER, TO> e2;
	
	public ConcatinatedExtract(NextGenExtractor<FROM, OVER> e1,NextGenExtractor<OVER,TO> e2) {
		this.e1=e1;
		this.e2=e2;
	}
	
	@Override
	public TO convert(FROM in) {
		return e2.convert(e1.convert(in));
	}

	public <OUT> NextGenExtractor<FROM, OUT> add(NextGenExtractor<TO, OUT> e3){
		return new ConcatinatedExtract<FROM,TO,OUT>(this, e3);
	}
	
}
