package ra;

import dao.Schema;
import dao.Tuple;

/**
 * Abstraction of a relational algebra
 * @author Asia
 *
 */
public interface Operator{
	//read one tuple from stream
	public Tuple readOneTuple();
	
	//public List<Tuple> readOneBlock();
	
	//reset all
	public void reset();
	
	public long getLength();
	
	
	public Schema getSchema();
	
	public void close();
}

