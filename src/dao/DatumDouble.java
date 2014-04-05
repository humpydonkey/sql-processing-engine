package dao;


public class DatumDouble extends Datum{
	private double value;
	
	public DatumDouble(String dataIn){
		super(DatumType.Double);
		value = Double.parseDouble(dataIn);
	}
	
	public DatumDouble(double dataIn){
		super(DatumType.Double);
		value = dataIn;
	}
	
	public double getValue(){
		return value;
	}
	
	@Override
	public double getNumericValue() {
		return value;
	}

	@Override
	public void setNumericValue(double valueIn) {
		value = valueIn;
	}
	
	@Override
	public String toString(){
			return String.valueOf(value);
	}

	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumDouble){
			DatumDouble obj = (DatumDouble)o;
			if(this.value>obj.value)
				return 1;
			else if(this.value<obj.value){
				return -1;
			}else
				return 0;
		} else if(o instanceof DatumLong){
			DatumLong obj = (DatumLong)o;
			if(this.value>obj.getValue())
				return 1;
			else if(this.value<obj.getValue()){
				return -1;
			}else
				return 0;
		}else{
			try {
				throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return -99;
		}
	}
	
	@Override
	public Datum clone() {
		Datum copy = new DatumDouble(value);
		return copy;
	}
}
