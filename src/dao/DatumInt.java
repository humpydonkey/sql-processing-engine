package dao;

public class DatumInt extends Datum{
	
	private int value;
	
	public DatumInt(String dataIn){
		super(DatumType.Int);
		value = Integer.parseInt(dataIn);
	}
	
	public DatumInt(int dataIn){
		super(DatumType.Int);
		value = dataIn;
	}
	
	public int getValue() {
		return value;
	}

	@Override
	public double getNumericValue() {
		return (double)value;
	}

	@Override
	public void setNumericValue(double valueIn) {
		value = (int)valueIn;
	}
}
