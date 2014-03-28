package ra;

import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import dao.Tuple;

public class OperatorSelection implements Operator{

	private Operator input;
	private Expression condition;
	
	public OperatorSelection(Operator inputIn,  Expression conditionIn){
		input = inputIn;
		condition = conditionIn;
	}
	

	@Override
	public List<Tuple> readOneBlock() {
		
		List<Tuple> selectedTuples = new LinkedList<Tuple>();

		List<Tuple> tuples = input.readOneBlock();
		for(Tuple tuple : tuples){
			
			EvaluatorConditionExpres evaluator = new EvaluatorConditionExpres(tuple);
			condition.accept(evaluator);
			if(evaluator.getResult()){
				selectedTuples.add(tuple);
			}
		}

	
		return selectedTuples;
	}
	
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = null;
		do{
			tuple = input.readOneTuple();
			if(tuple == null)
				return null;
			
			EvaluatorConditionExpres evaluator = new EvaluatorConditionExpres(tuple);
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
