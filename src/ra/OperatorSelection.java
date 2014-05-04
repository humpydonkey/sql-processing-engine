package ra;

import net.sf.jsqlparser.expression.Expression;
import dao.Datum;
import dao.Datum.CastError;
import dao.DatumDouble;
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
	private EvaluatorConditionExpres evaluator;
	
	public OperatorSelection(Operator inputIn,  Expression conditionIn){
		input = inputIn;
		condition = conditionIn;
		evaluator = new EvaluatorConditionExpres(null);
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
	
	public static void main(String[] args){

		Datum dd1 = new DatumDouble(0.6d);
		Datum dd2 = new DatumDouble(0.1d);

		try {
			double d1 = dd1.toDouble();
			double d2 = dd2.toDouble();
			DatumDouble dd3 = new DatumDouble(dd1.toDouble()-dd2.toDouble());
			
			System.out.println(d1-d2+"\n"+dd3.toDouble()+"\n"+dd3.toString());
		} catch (CastError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	@Override
	public void close() {
		input.close();
	}
}
