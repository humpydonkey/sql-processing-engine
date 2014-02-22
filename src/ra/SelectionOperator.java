package ra;

import net.sf.jsqlparser.expression.Expression;
import dao.Tuple;

public class SelectionOperator implements Operator{

	private Operator input;
	private Expression condition;
	
	public SelectionOperator(Operator inputIn,  Expression conditionIn){
		input = inputIn;
		condition = conditionIn;
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = null;
		do{
			tuple = input.readOneTuple();
			if(tuple == null)
				return null;
			
			Evaluator evaluator = new Evaluator(tuple);
			condition.accept(evaluator);
			if(!evaluator.getResult()){
				tuple = null;
			}
		
		}while(tuple == null);
		
		return tuple;
	}
	
	@Override
	public void reset() {
		input.reset();
	}
	
}
