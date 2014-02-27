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
		value = (long)valueIn;
	}
	
	
	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumLong){
			DatumLong obj = (DatumLong)o;
			if(this.value>obj.value)
				return 1;
			else if(this.value<obj.value){
				return -1;
			}else
				return 0;
		} else if(o instanceof DatumDouble){
			DatumDouble obj = (DatumDouble)o;
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
		Datum copy = new DatumLong(value);
		return copy;
	}
}
