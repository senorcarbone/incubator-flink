package org.apache.flink.streaming.api.invokable.operator;

public abstract class NextGenConversionAwareDeltaFunction<DATA,TO> implements NextGenDeltaFunction<DATA> {

	/**
	 * Generated Version ID
	 */
	private static final long serialVersionUID = 6927486219702689554L;
	private NextGenExtractor<DATA, TO> converter;
	
	public NextGenConversionAwareDeltaFunction(NextGenExtractor<DATA, TO> converter){
		this.converter=converter;
	}
	
	@SuppressWarnings("unchecked") //see comment below
	@Override
	public double getDelta(DATA oldDataPoint, DATA newDataPoint) {
		if (converter==null){
			//In case no conversion/extraction is required, we can cast DATA to TO
			//=> Therefore, "unchecked" warning is suppressed for this method.
			return getNestedDelta((TO)oldDataPoint,(TO)newDataPoint);
		} else {
			return getNestedDelta(converter.convert(oldDataPoint), converter.convert(newDataPoint));
		}
		
	}
	
	public abstract double getNestedDelta(TO oldDataPoint,TO newDataPoint);

}
