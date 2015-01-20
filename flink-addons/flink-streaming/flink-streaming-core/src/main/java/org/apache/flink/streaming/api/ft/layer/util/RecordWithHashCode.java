package org.apache.flink.streaming.api.ft.layer.util;

public class RecordWithHashCode {
	private byte[] serializedRecord;
	private int hashCode;

	public RecordWithHashCode(byte[] serializedRecord, int hashCode) {
		this.serializedRecord = serializedRecord;
		this.hashCode = hashCode;
	}

	public byte[] getSerializedRecord() {
		return serializedRecord;
	}

	public int getHashCode() {
		return hashCode;
	}

	public String toString() {
		String result = "[";
		for (int i = 0; i < serializedRecord.length; i++) {
			result += ", " + serializedRecord[i];
		}
		result = result.replaceFirst(", ", "");
		result += "]";
		return result;
	}

}
