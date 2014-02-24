package dao;

public class DatumFloat extends Datum{
	private float value;
	
	public DatumFloat(String dataIn){
		super(DatumType.Float);
		value = Float.parseFloat(dataIn);
	}
	
	public DatumFloat(float dataIn){
		super(DatumType.Float);
		value = dataIn;
	}
	
	public float getValue(){
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
	
	@Override
	public String toString(){
			return String.valueOf((int)value);
	}
}
