package org.apache.flink.streaming.paper.experiments;

public class AllTableShrink {

    private static String BASE_PATH="setups/";
    private static String[] INPUT_NAMES={
            "test-result-100c100t-AVERAGE",
            "test-result-50c50t-AVERAGE",
            "test-result-10c10t-AVERAGE",
    };
    private static String FILE_SUFFIX=".txt";

    public static void main(String[] args) throws Exception{

        for (int i=0;i<=8;i++){
            for (String INPUT_NAME:INPUT_NAMES){
                String[] parameters1={
                        BASE_PATH+INPUT_NAME+FILE_SUFFIX,
                        BASE_PATH+"barplot-agg-scen"+(i+1)+"-"+INPUT_NAME+FILE_SUFFIX,
                        ""+i,
                        "4",
                };

                TableShrink.main(parameters1);

                String[] parameters2={
                        BASE_PATH+INPUT_NAME+FILE_SUFFIX,
                        BASE_PATH+"barplot-mem-scen"+(i+1)+"-"+INPUT_NAME+FILE_SUFFIX,
                        ""+i,
                        "6",
                };

                TableShrink.main(parameters2);
            }
        }


    }

}
