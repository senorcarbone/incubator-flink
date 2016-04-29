package org.apache.flink.streaming.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class DEBSParser {

	private static final String file = "/Users/carbone/workspace/datasets/debs-full.csv";
	private static int numCols = 56;

	public static void main(String... args) throws Exception {
		BufferedWriter wr = new BufferedWriter(new FileWriter(file + "out"));
		int errlines = 0;
		int lines = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (line.split("\t").length == numCols) {
					wr.write(line + "\n");
				}
				else{
					errlines++;
				}
				lines++;
				 if(lines%10000 == 0){
					 System.out.println("Processed "+lines+" lines");
				 }
			}
		}
		System.out.println("Removed "+errlines+" lines");
		wr.close();
	}


}