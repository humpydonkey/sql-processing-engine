package dao;

/**
 * Long data cell
 * @author Asia
 *
 */
public class DatumLong extends Datum{
 
	private static final long serialVersionUID = -4625153744377548371L;
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
		}else
			throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");

	}
	
	
	@Override
	public Datum clone() {
		Datum copy = new DatumLong(value);
		return copy;
	}
	

	@Override
	public long getBytes() {
		return 64;
	}
	
	@Override
	public String toString(){
		return String.valueOf(value);
	}
	
}
