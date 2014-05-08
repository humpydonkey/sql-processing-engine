package dao;

import java.io.Serializable;
import java.sql.SQLException;



/**
 * Abstraction of a cell, the cell in a row in a table
 * @author Asia
 *
 */
public abstract class Datum implements Comparable<Datum>, Serializable {
	
	private static final long serialVersionUID = 1517024754320323043L;	
	
	public abstract String toString();
	public abstract boolean toBool() throws CastError;
	public abstract long toLong() throws CastError;
	public abstract double toDouble() throws CastError;
	
	public abstract boolean equals(Datum d)  throws CastError;
	public abstract DatumType getType();
	public abstract int hashCode();
	public abstract Datum clone();
	public abstract long getBytes();
	
	/**
	 * they are equal if they have the same value and type.
	 */
	@Override
	public boolean equals(Object obj){
		if(obj==null)
			return false;
		
		if(obj instanceof Datum){
			Datum compData = (Datum)obj;
			int compResult = this.compareTo(compData);
			return (compResult==0);
		}else{
			return false;	
		}
	}

	/**
	 * CastError gets thrown when we try to cast between two incompatible types
	 */
	public static class CastError extends SQLException {
		private static final long serialVersionUID = -2315013577552207034L;
		String from, to;
		public CastError(String from, String to) {
			super("Cast Error " + from + " -> " + to);
			this.from = from;
			this.to = to;
		}
	}

	
	
	/**
	 * Calculate two Datum objects by specific operation, only valid for Long and Double
	 * opId 1:Add; 2:Sub; 3:Multiply; 4:Division
	 * @param left : left Object
	 * @param right : right Object
	 * @param opId : Operation Id
	 * @return
	 */
	public static Datum calcDatum(Datum left, Datum right, int opId){
		DatumType type;
		if(left.getType()==DatumType.Long&&right.getType()==DatumType.Long)
			type = DatumType.Long;
		else
			type = DatumType.Double;
		
		Datum result = null;
		try{
			switch(opId){
			case 1:	//add
				if(type==DatumType.Long)
					result = new DatumLong(left.toLong()+right.toLong());
				else
					result = new DatumDouble(left.toDouble()+right.toDouble());
				break;
			case 2:	//sub
				if(type==DatumType.Long)
					result = new DatumLong(left.toLong()-right.toLong());
				else{
					double d = left.toDouble() - right.toDouble();
					if(d>0.0499999 && d<0.05d){
						d = 0.05;
					}
					result = new DatumDouble(d);
				}

				break;
			case 3:	//multiply
				if(type==DatumType.Long)
					result = new DatumLong(left.toLong()*right.toLong());
				else
					result = new DatumDouble(left.toDouble()*right.toDouble());
				break;
			case 4:	//division
				if(type==DatumType.Long){
					if(right.toDouble()==0d)
						result = new DatumLong(0);
					else
						result = new DatumLong(left.toLong()/right.toLong());
				}else{
					if(right.toDouble()==0d)
						result = new DatumDouble(0);
					else
						result = new DatumDouble(left.toDouble()/right.toDouble());
				}			
				break;
			default:
				throw new UnsupportedOperationException("Unsupported Operation id: "+opId); 
			}
		}catch(CastError e){
			e.printStackTrace();
		}
		
		return result;
	}
	
	
}
