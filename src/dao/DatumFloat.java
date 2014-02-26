package dao;


public class DatumFloat extends Datum{
	private double value;
	
	public DatumFloat(String dataIn){
		super(DatumType.Float);
		value = Double.parseDouble(dataIn);
	}
	
	public DatumFloat(double dataIn){
		super(DatumType.Float);
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
		if(o instanceof DatumFloat){
			DatumFloat obj = (DatumFloat)o;
			if(this.value>obj.value)
				return 1;
			else if(this.value<obj.value){
				return -1;
			}else
				return 0;
		}else if(o instanceof DatumInt || o instanceof DatumLong){
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
	
	@Override
	public Datum clone() {
		Datum copy = new DatumFloat(value);
		return copy;
	}
}
