package org.apache.flink.streaming.paper.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.LinkedList;

public class ToTikzQueryScale {

    public static void main (String[] args) throws Exception{
        String SCEN_NR="1";
        String MODE="mem";
        String[] INPUT_SIZES = {"10","50","100"};
        String INPUT_PATH_PREFIX="setups/barplot-"+MODE+"-scen"+SCEN_NR+"-test-result-";
        String INPUT_PATH_SUFFIX="-AVERAGE.txt";

        LinkedList<BufferedReader> reader=new LinkedList<BufferedReader>();
        for (String SIZE:INPUT_SIZES){
            reader.add(new BufferedReader(new FileReader(INPUT_PATH_PREFIX+SIZE+"c"+SIZE+"t"+INPUT_PATH_SUFFIX)));
        }

        double[][][] values=new double[INPUT_SIZES.length][2][3];
        for (int i=0; i<reader.size();i++){
            values[i]=parseResults(reader.get(i));
        }

        //iterate over lazy/eager

        for (int k=0;k<2;k++){
            System.out.println("*********************************************************");
            //Iterate Periodic, Deterministic, Arbitrary
            for (int i=0;i<3;i++){
                System.out.println("*******************");
                BufferedWriter bufferedWriter=new BufferedWriter(new FileWriter("setups/series-mem-"+SCEN_NR+"-"+k+"-"+i+".txt"));
                //Iterate over num queries
                for (int j=0;j<INPUT_SIZES.length;j++){
                    System.out.println(values[j][k][i]+"\t"+INPUT_SIZES[j]);
                    bufferedWriter.write(values[j][k][i]+"\t"+INPUT_SIZES[j]+"\n");
                }
                bufferedWriter.close();
            }
        }


        for (BufferedReader r:reader){
            r.close();
        }
    }

    private static double[][] parseResults(BufferedReader reader) throws Exception{
        double[][] results=new double[2][3];
        for (int i=0;i<2;i++){
            String line=reader.readLine();
            String[] cols=line.split("\t");
            for (int j=0;j<3;j++){
                results[i][j]=Double.valueOf(cols[j]);
            }
        }
        return results;
    }
}
