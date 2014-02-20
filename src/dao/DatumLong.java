package dao;

public class DatumLong extends Datum{
	private long value;
	
	public DatumLong(String dataIn){
		super(DatumType.Long);
		value = Long.parseLong(dataIn);
	}
	
	public DatumLong(long dataIn){
		super(DatumType.Long);
		value = dataIn;
	}

	public long getValue(){
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
