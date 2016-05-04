package org.apache.flink.streaming.api.operators.windowing;


import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class PairBasher {


	public static int minSlide = 50;
	public static int maxSlide = 60;

	public final static int queries = 20;
	public final static int range = 100;
	public final static int step = 10;
	public final static int repeats = 5;

	public final static String outFile = "pairbash.txt";


	public static void main(String[] args) {

		try {
			PrintWriter writer = new PrintWriter(outFile, "UTF-8");
			writer.flush();
			
			for (int i = 0; i < repeats; i++) {
				writer.println("RANGE "+range+" , MAXSLIDE "+maxSlide);
				List<PairPolicyGroup<Integer>> policyGroups = new ArrayList<>();
				for (int q = 0; q < queries; q++) {
					
					int slide = ThreadLocalRandom.current().nextInt(minSlide, maxSlide + 1);
					writer.println(slide);
					writer.flush();
					policyGroups.add(new PairPolicyGroup<>(
							new DeterministicPolicyGroup<>(
									new DeterministicCountTriggerPolicy<>(slide),
									new DeterministicCountEvictionPolicy<>(range))));
				}
				try {
					PairDiscretization.getCommonPairPolicy(policyGroups);
				} catch (OutOfMemoryError error) {
					//bashed!
					writer.println("FAILED");
					writer.flush();
					continue;
				} finally {
					maxSlide += step;
				}
				writer.println("PASSED");
				writer.flush();
			}
			
			writer.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
		}

	}
}
