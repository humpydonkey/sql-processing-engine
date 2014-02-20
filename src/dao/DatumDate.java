package dao;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatumDate extends Datum {
	private static DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	private Date value;
	
	public DatumDate(String dataIn) throws ParseException{
		super(DatumType.Date);
		value = format.parse(dataIn);
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
	

}
