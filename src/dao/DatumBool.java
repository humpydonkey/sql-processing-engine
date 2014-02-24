package dao;

public class DatumBool extends Datum {

	private boolean value;
	
	public DatumBool(String data) {
		super(DatumType.Bool);
		value = Boolean.parseBoolean(data);
	}
	
	public DatumBool(boolean data) {
		super(DatumType.Bool);
		value = data;
	}
	
	public boolean getValue(){
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
