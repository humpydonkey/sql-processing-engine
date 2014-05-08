package dao;


/**
 * String data cell
 * @author Asia
 *
 */
public class DatumString extends Datum {
 
	private static final long serialVersionUID = 6843611218396643951L;
	private String value;
	
	public DatumString(String dataIn){
		value = dataIn;
	}
	

	public DatumType getType(){return DatumType.String;}
	
	@Override
	public int hashCode(){
		return value.hashCode();
	}
	
	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumString)
			return value.compareTo(o.toString());
		 else
			throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");
	}
	
	@Override
	public Datum clone() {
		Datum copy = new DatumString(new String(value));
		return copy;
	}
	

	@Override
	public long getBytes() {
		return value.getBytes().length;
	}

	@Override
	public String toString() {
		return value;
	}


	@Override
	public boolean toBool() throws CastError {
		throw new CastError("Datum.String", "Datum.Bool");
	}


	@Override
	public long toLong() throws CastError {
		throw new CastError("Datum.String", "Datum.Long");
	}


	@Override
	public double toDouble() throws CastError {
		throw new CastError("Datum.String", "Datum.Double");
	}


	@Override
	public boolean equals(Datum d) throws CastError {
		if(d.getType()!=DatumType.String)
			return false;
		else
			return value.equals(d);
	}
}
