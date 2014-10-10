package org.apache.flink.streaming.util.nextGenExtractor;

import static org.junit.Assert.*;

import org.junit.Test;

public class FieldFromArrayTest {

	String[] testStringArray={"0","1","2","3","4"};
	Integer[] testIntegerArray={0,1,2,3,4};
	int[] testIntArray={0,1,2,3,4};
	
	@Test
	public void testStringArray() {
		for (int i=0;i<this.testStringArray.length;i++){
			assertEquals(this.testStringArray[i],new FieldFromArray<String>(i).extract(testStringArray));
		}
	}
	
	@Test
	public void testIntegerArray() {
		for (int i=0;i<this.testIntegerArray.length;i++){
			assertEquals(this.testIntegerArray[i],new FieldFromArray<String>(i).extract(testIntegerArray));
		}
	}
	
	@Test
	public void testIntArray() {
		for (int i=0;i<this.testIntArray.length;i++){
			assertEquals(new Integer(this.testIntArray[i]),new FieldFromArray<Integer>(i).extract(testIntArray));
		}
	}

}
