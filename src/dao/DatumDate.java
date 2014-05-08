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
	
	public int getYear(){ return y; }
	public int getMonth(){ return m; }
	public int getDay(){ return d; }
	public DatumType getType(){return DatumType.Date;}
	
	@Override
	public int hashCode(){
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
		try {
			if(equals(o))
				return 0;
			if(o instanceof DatumDate){
				return (int)(this.toLong()-o.toLong());		
			} else
				throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");
		} catch (CastError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
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
		return y*10000+m*100+d;
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
