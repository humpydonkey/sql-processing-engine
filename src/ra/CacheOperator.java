package ra;

import java.util.List;

import dao.Tuple;

public class CacheOperator implements Operator {
	
	List<Tuple> tuples;
	
	public CacheOperator(List<Tuple> tuplesIn){
		tuples = tuplesIn;
	}
	
	@Override
	public Tuple readOneTuple() {
		return tuples.iterator().next();
	}

	@Override
	public void reset() {
		tuples=null;
	}

}
