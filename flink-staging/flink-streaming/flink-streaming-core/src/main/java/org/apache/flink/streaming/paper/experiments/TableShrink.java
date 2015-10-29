package org.apache.flink.streaming.paper.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class TableShrink {

    private static String INPUT;
    private static String OUTPUT;
    private static int CASE;
    private static int SELECTED_COL;

    public static void main (String[] args) throws Exception{
        if (args.length==4){
            INPUT=args[0];
            OUTPUT=args[1];
            CASE=Integer.valueOf(args[2]);
            SELECTED_COL=Integer.valueOf(args[3]);
        } else {
            INPUT="setups/test-result-50c50t-AVERAGE.txt";
            OUTPUT="setups/barplot-agg-50c50t.txt";
            CASE=0;
            SELECTED_COL=3;
        }

        BufferedReader inFile=new BufferedReader(new FileReader(INPUT));
        BufferedWriter outFile=new BufferedWriter(new FileWriter(OUTPUT));

        int[][] selectedRows={{1,2,3},{6,7,8}};

        String line;
        int i=0;
        int row=0;
        int col=0;
        while ((line=inFile.readLine())!=null){

            try{
                i=Integer.valueOf(line.split("\t")[1]);
            } catch (NumberFormatException e){
                continue;
            }

            try{
                int j=Integer.valueOf(line.split("\t")[0]);
                if (j!=CASE){
                    continue;
                }
            } catch (NumberFormatException e){
                continue;
            }


            if (selectedRows[row][col]==i){
                outFile.write(line.split("\t")[SELECTED_COL]+"\t");
                col++;
            }

            if (col>=selectedRows[row].length){
                outFile.newLine();
                row++;
                col=0;
            }

            if (row>=selectedRows.length){
                outFile.newLine();
                break;
            }
        }

        inFile.close();
        outFile.close();
    }

}
