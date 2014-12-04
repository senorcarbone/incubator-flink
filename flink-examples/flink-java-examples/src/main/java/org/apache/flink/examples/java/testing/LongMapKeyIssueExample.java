package org.apache.flink.examples.java.testing;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.api.java.ExecutionEnvironment;

public class LongMapKeyIssueExample {

    public static void main(String[] args) throws Exception {
    
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.fromElements(new Results(), new Results()).print();        
        env.execute();        
    }
   public  static class Results implements Serializable{
    	private static final long serialVersionUID = 6948869822006827228L;
    	private HashMap<Long,Integer> records;
    	public Results(){
    		records = new HashMap<Long, Integer>();
    	}
    	public Results(HashMap<Long,Integer> records){
    		setRecords(records);
    	}
    	public HashMap<Long, Integer> getRecords() {
    		return records;
    	}
    	public void setRecords(HashMap<Long, Integer> records) {
    		this.records = records;
    	}
    }
}
