package org.apache.flink.streaming.examples.sampling;


import java.util.Random;

public class SamplingUtils {
	
	private static Random gen = new Random();

	public static boolean flip(int sides) {
		return (gen.nextDouble() * sides < 1);
	}

}
