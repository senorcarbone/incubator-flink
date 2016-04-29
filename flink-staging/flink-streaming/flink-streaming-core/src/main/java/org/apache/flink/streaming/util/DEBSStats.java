package org.apache.flink.streaming.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class DEBSStats {

	private static final String file = "/Users/carbone/workspace/datasets/debs-fullcleaned.csv";
	//private static final String file = "/home/jtraub/Downloads/DEBS-Data/20120222-171330-000077.txt";
	private static int numCols = 56;
	private static int noBooleanFields = 13;

	public static void main(String... args) throws Exception {
		int[] windowCounter=new int[numCols-noBooleanFields];
		int[] windowSize=new int[numCols-noBooleanFields];
		int[] largestWindow=new int[numCols-noBooleanFields];
		int[] smallestWindow=new int[numCols-noBooleanFields];
		boolean[] previouseValue=new boolean[numCols-noBooleanFields];
		boolean firstRow=true;

		BufferedWriter wr = new BufferedWriter(new FileWriter(file + ".out"));
		BufferedWriter[] windowSizesWriter = new BufferedWriter[numCols-noBooleanFields];
		for (int i=0; i<numCols-noBooleanFields;i++){
			windowSizesWriter[i]=new BufferedWriter(new FileWriter(file +".col"+i+".windows"));
			largestWindow[i]=Integer.MIN_VALUE;
			smallestWindow[i]=Integer.MAX_VALUE;
		}

		int errlines = 0;
		int lines = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] lineSplits;
				if ((lineSplits=line.split("\t")).length == numCols) {
					wr.write(line + "\n");
					for (int i=noBooleanFields-1;i<numCols-1;i++){

						//i starts from noBooleanFields-1
						//j starts from 0
						int j=i-noBooleanFields+1;

						if (previouseValue[j]!=lineSplits[i].equals("1")||firstRow){
							//A new window started
							windowCounter[j]++;
							if (windowSize[j]>largestWindow[j]){
								largestWindow[j]=windowSize[j];
							}
							if (windowSize[j]<smallestWindow[j]&&!firstRow){
								smallestWindow[j]=windowSize[j];
							}
							if (!firstRow) {
								windowSizesWriter[j].write(windowSize[j] + "\n");
							}
							//Reset window size
							windowSize[j]=0;
						} else {
							//Still the same windows (just count item)
							windowSize[j]++;
						}

						//Remember current state to detect window ends
						previouseValue[j]=lineSplits[i].equals("1");
					}
				}
				else{
					errlines++;
				}
				lines++;
				if(lines%10000 == 0){
					System.out.println("Processed "+lines+" lines");
				}

				firstRow=false;
			}
		}
		System.out.println("Removed "+errlines+" lines");
		wr.close();
		for (int i=0; i<numCols-noBooleanFields;i++) {
			windowSizesWriter[i].close();
		}

		//Write col stats
		BufferedWriter wr2 = new BufferedWriter(new FileWriter(file + ".stats"));
		for (int i=0; i<numCols-noBooleanFields; i++){
			wr2.write("Field "+(i+noBooleanFields)+" (0-based) \n");
			wr2.write("\tNum Win: "+windowCounter[i]+"\n");
			wr2.write("\tMax Win: "+largestWindow[i]+"\n");
			wr2.write("\tMin Win: "+smallestWindow[i]+"\n");
		}
		wr2.close();
	}


}