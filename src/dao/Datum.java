package dao;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Datum {
	private static DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	private String data;
	
	public Datum(String dataIn){
		data = dataIn;
	}
	
	public long toLong(){
		return Long.parseLong(data);
	}
	
	public int toInt(){
		return Integer.parseInt(data);
	}
	
	public boolean toBool(){
		return Boolean.parseBoolean(data);
	}
	
	public float toFloat(){
		return Float.parseFloat(data);
	}
	
	public String toString(){
		return data;
	}
	
	public Date toDate(){
		try {
			return format.parse(data);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} 
	}
	
}
