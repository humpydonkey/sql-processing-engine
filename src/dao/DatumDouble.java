package dao;


/**
 * Double data cell
 * @author Asia
 *
 */
public class DatumDouble extends Datum{
 
	private static final long serialVersionUID = 6695592229704647336L;
	private double value;
	
	public DatumDouble(double dataIn){
		value = dataIn;
	}
	
	public void setValue(double val){value = val;}
	
	public DatumType getType(){return DatumType.Double;}

	@Override
	public int getHashValue(){
		//TODO Assume the precision is enough, too strong assumption
		return (int)(value*100000000);
	}
	
	@Override
	public int compareTo(Datum o) {
		try {
			if(equals(o))
				return 0;
			if(o instanceof DatumDouble || o instanceof DatumLong){
				double diff =  value - o.toDouble();
				if(diff==0)
					return 0;
				else
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
		Datum copy = new DatumDouble(value);
		return copy;
	}
	

	@Override
	public long getBytes() {
		return 8;
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}

	@Override
	public boolean toBool() throws CastError {
		throw new CastError("Datum.Double","Datum.Bool");
	}

	@Override
	public long toLong() throws CastError {
		return (long)value;
	}

	@Override
	public double toDouble() throws CastError {
		return value;
	}

	@Override
	public boolean equals(Datum d) throws CastError {
		if(d.getType()!=DatumType.Double)
			return false;
		else
			return value==d.toDouble();
	}
}
