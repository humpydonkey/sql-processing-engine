package dao;

/**
 * Boolean data cell
 * @author Asia
 *
 */
public class DatumBool extends Datum {

	private static final long serialVersionUID = -318289153287331084L;
	private boolean value;	
	
	public DatumType getType(){return DatumType.Bool;}
	
	public DatumBool(boolean data) {
		value = data;
	}
	
	@Override
	public int hashCode(){
		return value==true?1:0;
	}


	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumBool){
			DatumBool obj = (DatumBool)o;
			if(this.value==obj.value)
				return 0;
			else if(this.value==true){
				return 1;
			}else
				return -1;
		} else
			throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");

	}

	@Override
	public Datum clone() {
		Datum copy = new DatumBool(this.value);
		return copy;
	}

	@Override
	public long getBytes() {
		return 1;
	}

	@Override
	public String toString() {
		return value?"true":"false";
	}

	@Override
	public boolean toBool() throws CastError {
		return value;
	}

	@Override
	public long toLong() throws CastError {
		throw new CastError("Datum.Bool","Datum.Long");
	}

	@Override
	public double toDouble() throws CastError {
		throw new CastError("Datum.Bool","Datum.Double");
	}

	@Override
	public boolean equals(Datum d) throws CastError {
		if(d.getType()!=DatumType.Bool)
			return false;
		else
			return value==d.toBool();
	}
}
