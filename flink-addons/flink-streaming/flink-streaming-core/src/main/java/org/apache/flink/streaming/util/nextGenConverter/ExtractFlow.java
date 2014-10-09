package org.apache.flink.streaming.util.nextGenConverter;

import org.apache.flink.streaming.api.invokable.operator.NextGenTypeExtractor;

public class ExtractFlow<FROM, OVER, TO> implements NextGenTypeExtractor<FROM, TO>{

	/**
	 * auto-generated version id
	 */
	private static final long serialVersionUID = -7603861976991811321L;
	NextGenTypeExtractor<FROM, OVER> c1;
	NextGenTypeExtractor<OVER,TO> c2;
	
	public ExtractFlow(NextGenTypeExtractor<FROM, OVER> c1,NextGenTypeExtractor<OVER, TO> c2) {
		this.c1=c1;
		this.c2=c2;
	}
	
	static <FROM,OVER,TO> ExtractFlow<FROM, OVER, TO> extractFlow(NextGenTypeExtractor<FROM, OVER> c1,NextGenTypeExtractor<OVER, TO> c2){
		return new ExtractFlow<FROM, OVER, TO>(c1,c2);
	}
	
	@Override
	public TO convert(FROM in) {
		return c2.convert(c1.convert(in));
	}

	public <RES> ExtractFlow<FROM, TO, RES> add(NextGenTypeExtractor<TO, RES> c){
		return new ExtractFlow<FROM, TO, RES>(this,c);
	}
}
