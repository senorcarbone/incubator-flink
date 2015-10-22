package org.apache.flink.streaming.paper.experiments;

import org.apache.flink.streaming.api.windowing.helper.SystemTimestamp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.LinkedList;

public class AverageMultipleExp {

    private final static int RESULT_START_ID=0;
    private final static int RESULT_END_ID=9;
    private final static String RESULT_PREFIX="setups/test-result-100c100t-";
    private final static String FILETYPE_SUFFIX=".txt";

    public static void main (String args[]) throws Exception{

        LinkedList<BufferedReader> fileReader= new LinkedList<BufferedReader>();
        BufferedWriter avgWriter= new BufferedWriter(new FileWriter(RESULT_PREFIX+"AVERAGE"+FILETYPE_SUFFIX));
        BufferedWriter sdvWriter= new BufferedWriter(new FileWriter(RESULT_PREFIX+"STDDEV"+FILETYPE_SUFFIX));

        for (int i=RESULT_START_ID; i<=RESULT_END_ID; i++){
            fileReader.add(new BufferedReader(new FileReader(RESULT_PREFIX+i+FILETYPE_SUFFIX)));
        }

        //skip the header
        String header="";
        for (BufferedReader reader:fileReader){
            header=reader.readLine();
        }
        avgWriter.write(header+"\n");
        sdvWriter.write(header+"\n");

        outer: while (true){
            LinkedList<String> lines=new LinkedList<String>();

            //read all lines as string
            for (BufferedReader reader:fileReader){
                String line=reader.readLine();
                if (line==null){
                    break outer;
                } else {
                    lines.add(line);
                }
            }

            //parse the lines to doubles
            double[][] rows=new double[lines.size()][lines.getFirst().split("\t").length-2];
            for (int i=0; i<lines.size();i++){
                String line=lines.get(i);
                String[] splittedFields=line.split("\t");
                for (int j=2; j<splittedFields.length; j++){
                    rows[i][j-2]=Double.valueOf(splittedFields[j]);
                }
            }

            //Calcualate the sums
            double[] sums=new double[rows[0].length];
            for (int i=0; i<rows.length; i++){

                for (int j=0; j<rows[0].length; j++){
                    sums[j]+=rows[i][j];
                }
            }

            //Calculate the averages
            double[] avg=new double[rows[0].length];
            for (int i=0;i<sums.length;i++){
                avg[i]=sums[i]/(double)rows.length;
            }

            //Calculate the standard deviations
            double[] tmp=new double[rows[0].length];
            for (int i=0; i<rows.length; i++){
                for (int j=0; j<rows[0].length; j++){
                    tmp[j]+=Math.pow(rows[i][j]-avg[j],2);
                }
            }
            double[] sdv=new double[rows[0].length];
            for (int i=0;i<sums.length;i++){
                sdv[i]=Math.sqrt(tmp[i]/(double)rows.length);
            }

            //write results
            String tmp1 = lines.getFirst().split("\t")[0];
            String tmp2 = lines.getFirst().split("\t")[1];
            avgWriter.write(tmp1+"\t"+tmp2);
            sdvWriter.write(tmp1+"\t"+tmp2);
            for (int i=0;i<sums.length;i++){
                avgWriter.write("\t"+avg[i]);
                sdvWriter.write("\t"+sdv[i]);
            }
            avgWriter.newLine();
            sdvWriter.newLine();

        }

        for (BufferedReader reader:fileReader){
            reader.close();
        }
        avgWriter.close();
        sdvWriter.close();

    }

}
