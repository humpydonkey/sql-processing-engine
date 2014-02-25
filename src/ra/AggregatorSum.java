package ra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import dao.Datum;
import dao.Tuple;

public class AggregatorSum extends Aggregator {

	private Map<String, Datum> sumMap;
	private Expression paraExpr;
	
	public AggregatorSum(Function funcIn, String[] groupByNamesIn) {
		super(funcIn, groupByNamesIn);
		
		sumMap = new HashMap<String, Datum>();
		
		@SuppressWarnings("unchecked")
		List<Expression> paraList = func.getParameters().getExpressions();
		if(paraList.size()>1)
			throw new UnsupportedOperationException("Not supported yet."); 
		else
			paraExpr = paraList.get(0);
	}

	@Override
	public void aggregate(Tuple tuple, String key) {
		Evaluator eval = new Evaluator(tuple);
		paraExpr.accept(eval);
		Datum data = eval.copyDatum();
		
		if(!sumMap.containsKey(key)){
			//insert new
			sumMap.put(key, data);
		}else{
			//update old, sum
			Datum old = sumMap.get(key);
			//can not sum Bool, String, Date
			double oldVal = old.getNumericValue();
			double newVal = data.getNumericValue();
			old.setNumericValue(oldVal+newVal);
		}
	}

	@Override
	public Datum getValue(String key) {
		return sumMap.get(key);
	}

}
