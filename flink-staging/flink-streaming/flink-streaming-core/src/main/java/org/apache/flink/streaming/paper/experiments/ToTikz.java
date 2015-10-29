package org.apache.flink.streaming.paper.experiments;

import java.io.BufferedReader;
import java.io.FileReader;

public class ToTikz {

    public static void main (String[] args) throws Exception{

        String MODE="agg";

        String INPUT_SCEN_1="setups/barplot-"+MODE+"-scen1-test-result-100c100t-AVERAGE.txt";
        String INPUT_SCEN_7="setups/barplot-"+MODE+"-scen7-test-result-100c100t-AVERAGE.txt";
        String INPUT_SCEN_9="setups/barplot-"+MODE+"-scen9-test-result-100c100t-AVERAGE.txt";

        BufferedReader s1Reader=new BufferedReader(new FileReader(INPUT_SCEN_1));
        BufferedReader s7Reader=new BufferedReader(new FileReader(INPUT_SCEN_7));
        BufferedReader s9Reader=new BufferedReader(new FileReader(INPUT_SCEN_9));

        double[][] scen1Results=parseResults(s1Reader);
        double[][] scen7Results=parseResults(s7Reader);
        double[][] scen9Results=parseResults(s9Reader);

        System.out.println(
                "*******************\n"+
                "RESULT FOR MODE "+MODE+"\n"+
                "*******************\n\n"+
                "*******************\n"+
                "LAZY\n"+
                "*******************\n\n"+
                writeTikz(scen1Results,scen7Results,scen9Results,false)+
                "\n\n"+
                "*******************\n"+
                "EAGER\n"+
                "*******************\n\n"+
                writeTikz(scen1Results,scen7Results,scen9Results,true)
        );

        s1Reader.close();
        s7Reader.close();
        s9Reader.close();

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

    private static String writeTikz(double[][] results1,double[][] results7,double[][] results9,boolean eager){
        int i=eager?1:0;

        return ""+
        "\\addplot %[fill=blue!30!white, postaction={pattern=north east lines}]\n" +
        "    coordinates {\n" +
        "        (2010,"+results7[i][0]+") %Periodic - SCEN 7\n" +
        "        (2011,"+results1[i][0]+") %Periodic - SCEN 1\n" +
        "        (2012,"+results9[i][0]+") %Periodic - SCEN 9\n" +
        "        (2013,0)};\n" +
        "\\addplot %[fill=red!30!white, postaction={pattern=crosshatch}]\n" +
        "    coordinates {\n" +
        "        (2010,"+results7[i][1]+") %Derterministic - SCEN 7\n" +
        "        (2011,"+results1[i][1]+") %Derterministic - SCEN 1\n" +
        "        (2012,"+results9[i][1]+") %Derterministic - SCEN 9\n" +
        "        (2013,0)};\n" +
        "\\addplot %[fill=brown!30!white, postaction={pattern=north west lines}]\n" +
        "    coordinates {\n" +
        "        (2010,"+results7[i][2]+") %Arbitrary - SCEN 7\n" +
        "        (2011,"+results1[i][2]+") %Arbitrary - SCEN 1\n" +
        "        (2012,"+results9[i][2]+") %Arbitrary - SCEN 9\n" +
        "        (2013,0)};";
    }


}
