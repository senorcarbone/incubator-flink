package org.apache.flink.streaming.util.serialization;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class NextGenOutTypeWrapper<OUT> extends TypeWrapper<Tuple2<OUT, String[]>>{

	private static final long serialVersionUID = 1L;
	//Info about OUT
	private boolean outIsBasicType;
	private boolean outIsTupleType;
	private int outArity;
	private Class<OUT> outTypeClass;
	private boolean outIsKeyType;
	private TypeSerializer<OUT> outTypeSerializer;
	
	public NextGenOutTypeWrapper(TypeWrapper<OUT> outTypeWrapper){
		this(
			outTypeWrapper.getTypeInfo().isBasicType(),
			outTypeWrapper.getTypeInfo().isTupleType(),
			outTypeWrapper.getTypeInfo().getArity(),
			outTypeWrapper.getTypeInfo().getTypeClass(),
			outTypeWrapper.getTypeInfo().isKeyType(),
			outTypeWrapper.getTypeInfo().createSerializer()
		);
	}
	
	public NextGenOutTypeWrapper(boolean outIsBasicType,
			boolean outIsTupleType, int outArity, Class<OUT> outTypeClass,
			boolean outIsKeyType, TypeSerializer<OUT> outTypeSerializer) {
		super();
		this.outIsBasicType = outIsBasicType;
		this.outIsTupleType = outIsTupleType;
		this.outArity = outArity;
		this.outTypeClass = outTypeClass;
		this.outIsKeyType = outIsKeyType;
		this.outTypeSerializer = outTypeSerializer;
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException,
	ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}
	
	@Override
	protected void setTypeInfo() {
		String[] sampleStringArray={""};
		typeInfo=new TupleTypeInfo<Tuple2<OUT, String[]>>(
				new TypeInformation<OUT>() {
					@Override
					public boolean isBasicType() {return outIsBasicType;}
					@Override
					public boolean isTupleType() {return outIsTupleType;}
					@Override
					public int getArity() {return outArity;}
					@Override
					public Class<OUT> getTypeClass() {return outTypeClass;}
					@Override
					public boolean isKeyType() {return outIsKeyType;}
					@Override
					public TypeSerializer<OUT> createSerializer() {return outTypeSerializer;}
				},
				new ObjectTypeWrapper<String[]>(sampleStringArray).getTypeInfo()
		);
	}
}