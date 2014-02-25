package dao;

import java.util.Date;

public class DatumString extends Datum {
	private String value;
	
	public DatumString(String dataIn){
		super(DatumType.String);
		value = dataIn;
	}
	
	public String getValue(){
		return value;
	}
	
	public void setValue(String str){
		value = str;
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
	public int compareTo(Datum o) {
		if(o instanceof DatumString){
			DatumString obj = (DatumString)o;
			String obj1 = this.getValue();
			String obj2 = obj.getValue();
			return obj1.compareTo(obj2);
		} else{
			try {
				throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		}
	}
	
	@Override
	public Datum clone() {
		Datum copy = new DatumString(new String(value));
		return copy;
	}
}
