package ra;

import java.util.Iterator;
import java.util.List;

import dao.Tuple;

public class OperatorCache implements Operator {
	
	List<Tuple> tuples;
	Iterator<Tuple> iter;
	
	public OperatorCache(List<Tuple> tuplesIn){
		tuples = tuplesIn;
		iter = tuples.iterator();
	}
	
	@Override
	public Tuple readOneTuple() {
		if(iter.hasNext())
			return iter.next();
		else
			return null;
	}

	@Override
	public void reset() {
		iter = tuples.iterator();
	}

}
