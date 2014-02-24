package dao;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatumDate extends Datum {
	private static DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	private Date value;
	
	public DatumDate(String dataIn){
		super(DatumType.Date);
		try {
			value = format.parse(dataIn);
		} catch (ParseException e) {
			value = null;
			e.printStackTrace();
		}
	}
	
	public DatumDate(Date dataIn){
		super(DatumType.Date);
		value = dataIn;
	}
	
	public Date getValue(){
		return value;
	}
	
	@Override
	public double getNumericValue() {
		System.out.println("Wrong get value.");
		return 0;
	}

	@Override
	public void setNumericValue(double valueIn) {
		System.out.println("Wrong set value.");
	}
	
	@Override
	public String toString(){
		return format.format(value);
	}
	

}
