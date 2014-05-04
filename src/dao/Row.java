package dao;

public class Row {
	private Datum[] data;
	public Row(Datum[] dataIn){
		data = dataIn;
	}
	
	public Datum[] getData(){return data;}
	
}
