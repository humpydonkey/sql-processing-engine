package dao;

/**
 * Date data cell
 * @author Asia
 *
 */
public class DatumDate extends Datum {

	private static final long serialVersionUID = 4697762788088326757L;

	private int y;
	private int m;
	private int d;
	
	public DatumDate(int year, int month, int day){
		y = year;
		m = month;
		d = day;
	}
	
	public DatumType getType(){return DatumType.Date;}
	
	@Override
	public int getHashValue(){
		try {
			return (int) toLong();
		} catch (CastError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return toString().hashCode();
	}
	
	
	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumDate){
			DatumDate obj = (DatumDate)o;
			try {
				return (int)(this.toLong()-obj.toLong());
			} catch (CastError e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		} else
			throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");

	}
	
	@Override
	public Datum clone() {
		return new DatumDate(y, m, d);
	}
	

	@Override
	public long getBytes() {
		return 12;
	}

	@Override
	public long toLong() throws CastError {
		return y*1000+m*100+d;
	}

	@Override
	public double toDouble() throws CastError {
		return toLong();
	}

	@Override
	public boolean equals(Datum d) throws CastError {
		if(d.getType()!=DatumType.Date)
			return false;
		else
			return this.toLong()==d.toLong();
	}

	@Override
	public String toString() {
		return String.format("%04d-%02d-%02d", y, m, d);
	}

	@Override
	public boolean toBool() throws CastError {
		throw new CastError("Datum.Date","Datum.Bool");
	}

}
