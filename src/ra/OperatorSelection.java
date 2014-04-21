package ra;

import net.sf.jsqlparser.expression.Expression;
import dao.Schema;
import dao.Tuple;

/**
 * Operator selection, filter tuples that do not
 * satisfy the condition in where
 * @author Asia
 *
 */
public class OperatorSelection implements Operator{

	private Operator input;
	private Expression condition;
	private EvaluatorConditionExpres_Pure evaluator;
	
	public OperatorSelection(Operator inputIn,  Expression conditionIn){
		input = inputIn;
		condition = conditionIn;
		evaluator = new EvaluatorConditionExpres_Pure(null);
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = null;
		while((tuple=input.readOneTuple())!=null){
			evaluator.updateTuple(tuple);
			condition.accept(evaluator);
			if(evaluator.getResult())
				break;		
		}
		return tuple;
	}
	
	
	@Override
	public void reset() {
		input.reset();
	}

	@Override
	public long getLength() {
		return input.getLength();
	}
	

	@Override
	public Schema getSchema() {
		return input.getSchema();
	}
}
