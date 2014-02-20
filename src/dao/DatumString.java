package dao;

public class DatumString extends Datum {
	private String value;
	
	public DatumString(String dataIn){
		super(DatumType.String);
		value = dataIn;
	}
	
	public String getValue(){
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
