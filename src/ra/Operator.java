package ra;

import java.util.List;

import dao.Tuple;

public interface Operator{
	//read one tuple from stream
	public Tuple readOneTuple();
	
	//public List<Tuple> readOneBolck();
	
	//reset all
	public void reset();
}

