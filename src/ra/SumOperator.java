package ra;

import net.sf.jsqlparser.expression.Function;
import dao.Datum;
import dao.Tuple;

public class SumOperator implements Operator{

	private Operator input;
	private Datum sum;
	private Function func;

	public SumOperator(Operator inputIn, Function funcIn){
		input = inputIn;
		sum = null;
		func = funcIn;
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();
		
		Evaluator eval = new Evaluator(tuple);
		
		return tuple;
	}

	@Override
	public void reset() {
		input.reset();
	}

}
