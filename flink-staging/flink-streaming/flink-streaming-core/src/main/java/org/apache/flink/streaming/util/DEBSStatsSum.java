package org.apache.flink.streaming.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

public class DEBSStatsSum {

	private static final String file = "/Users/carbone/workspace/datasets/debs-fullcleaned.csv";
	private static int numCols = 56;
	private static int noBooleanFields = 13;

	private static List<Integer> filters = Arrays.asList(18, 19, 20, 21, 22, 23, 24, 25, 26, 30, 31, 33, 34, 35, 36, 37, 38, 48, 49, 50);

	public static void main(String... args) throws Exception {
		int[] windowCounter = new int[numCols - noBooleanFields];
		int[] windowSize = new int[numCols - noBooleanFields];
		boolean[] previouseValue = new boolean[numCols - noBooleanFields];
		boolean firstRow = true;

		BufferedWriter windowSizesWriter = new BufferedWriter(new FileWriter(file + "-outliers"));

		int errlines = 0;
		int lines = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] lineSplits;
				if ((lineSplits = line.split("\t")).length == numCols) {
					for (int i = noBooleanFields - 1; i < numCols - 1; i++) {

						//i starts from noBooleanFields-1
						//j starts from 0
						int j = i - noBooleanFields + 1;

						if (previouseValue[j] != lineSplits[i].equals("1") || firstRow) {
							//A new window started
							windowCounter[j]++;
							if (!firstRow && filters.contains(i)) {
								if (windowSize[j] > 33995) {
									int numChunks = windowSize[j] / 33995;
									for (int k = 0; k < numChunks; k++) {
										windowSizesWriter.write("33995\n");
									}

								} else {
									windowSizesWriter.write(windowSize[j] + "\n");
								}
							}
							//Reset window size               
							windowSize[j] = 0;
						} else {
							//Still the same windows (just count item)
							windowSize[j]++;
						}

						//Remember current state to detect window ends
						previouseValue[j] = lineSplits[i].equals("1");
					}
				} else {
					errlines++;
				}
				lines++;
				if (lines % 10000 == 0) {
					System.out.println("Processed " + lines + " lines");
				}

				firstRow = false;
			}
		}
		System.out.println("Removed " + errlines + " lines");
		for (int i = 0; i < numCols - noBooleanFields; i++) {
			windowSizesWriter.close();
		}

		//Write col stats
		BufferedWriter wr2 = new BufferedWriter(new FileWriter(file + ".stats"));
		for (int i = 0; i < numCols - noBooleanFields; i++) {
			wr2.write("Field " + (i + noBooleanFields) + " (0-based) \n");
			wr2.write("\tNum Win: " + windowCounter[i] + "\n");
		}
		wr2.close();
	}

}