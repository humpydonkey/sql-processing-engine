package dao;


/**
 * Long data cell
 * @author Asia
 *
 */
public class DatumLong extends Datum{
 
	private static final long serialVersionUID = -4625153744377548371L;
	private long value;
	
	public DatumLong(long dataIn){
		value = dataIn;
	}

	public void setValue(long val){ value = val; }
	
	public DatumType getType(){return DatumType.Long;}
	
	@Override
	public int getHashValue(){
		return (int)value;
	}
	
	
	@Override
	public int compareTo(Datum o) {
		try {
			if(equals(o))
				return 0;
			if(o instanceof DatumDouble || o instanceof DatumLong){
				double diff =  value - o.toDouble();
				return diff>0?1:-1;
			}else
				throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");
		} catch (CastError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	
	@Override
	public Datum clone() {
		Datum copy = new DatumLong(value);
		return copy;
	}
	

	@Override
	public long getBytes() {
		return 8;
	}
	
	@Override
	public String toString(){
		return String.valueOf(value);
	}
	

	@Override
	public boolean toBool() throws CastError {
		throw new CastError("DatumLong", "DatumBool");
	}


	@Override
	public long toLong() throws CastError {
		return value;
	}


	@Override
	public double toDouble() throws CastError {
		return (double)value;
	}


	@Override
	public boolean equals(Datum d) throws CastError {
		if(d.getType()!=DatumType.Long)
			return false;
		else
			return value==d.toLong();
	}
}
