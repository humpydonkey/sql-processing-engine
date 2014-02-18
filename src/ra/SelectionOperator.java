package ra;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import dao.Tuple;

public class SelectionOperator implements Operator{

	private Operator input;
	private Column[] schema;
	private Expression condition;
	
	public SelectionOperator(Operator inputIn, Column[] schemaIn, Expression conditionIn){
		input = inputIn;
		schema = schemaIn;
		condition = conditionIn;
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = null;
		do{
			tuple = input.readOneTuple();
			if(tuple == null)
				return null;
			
			Evaluator evaluator = new Evaluator(schema, tuple);
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
