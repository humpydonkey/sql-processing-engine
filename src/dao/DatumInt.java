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
	
	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumInt){
			DatumInt obj = (DatumInt)o;
			if(this.value>obj.value)
				return 1;
			else if(this.value<obj.value){
				return -1;
			}else
				return 0;
		} else if(o instanceof DatumLong || o instanceof DatumFloat){
			double obj = o.getNumericValue();
			if(this.value>obj)
				return 1;
			else if(this.value<obj){
				return -1;
			}else
				return 0;
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
}
