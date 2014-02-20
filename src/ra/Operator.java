package ra;

import dao.Schema;
import dao.Tuple;

public interface Operator{
	//read one tuple from stream
	public Tuple readOneTuple();
	
	//reset all
	public void reset();
}

