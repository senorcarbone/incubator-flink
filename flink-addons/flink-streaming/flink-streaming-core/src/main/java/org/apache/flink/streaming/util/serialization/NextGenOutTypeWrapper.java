package org.apache.flink.streaming.util.serialization;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class NextGenOutTypeWrapper<OUT1,OUT2> extends TypeWrapper<Tuple2<OUT1, OUT2>>{

	private static final long serialVersionUID = 1L;
	//Info about OUT
	private TypeWrapper<OUT1> outTypeWrapper1;
	private TypeWrapper<OUT2> outTypeWrapper2;
	
	public NextGenOutTypeWrapper(TypeWrapper<OUT1> outTypeWrapper1,TypeWrapper<OUT2> outTypeWrapper2){
		this.outTypeWrapper1=outTypeWrapper1;
		this.outTypeWrapper2=outTypeWrapper2;
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException,
	ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}
	
	@Override
	protected void setTypeInfo() {
		typeInfo=new TupleTypeInfo<Tuple2<OUT1, OUT2>>(
				outTypeWrapper1.getTypeInfo(),
				outTypeWrapper2.getTypeInfo()
		);
	}
}