package ra;

import dao.Datum;
import dao.DatumFloat;
import dao.Tuple;
import net.sf.jsqlparser.expression.Function;

public class AggregatorAvg extends Aggregator{

	private Aggregator sumer;
	private Aggregator counter;
	
	public AggregatorAvg(Function funcIn, String[] groupByNamesIn) {
		super(funcIn, groupByNamesIn);
		sumer = new AggregatorSum(funcIn, groupByNamesIn);
		counter = new AggregatorCount(funcIn, groupByNamesIn);
	}
	
	@Override
	public Datum getValue(String key) {
		Datum total = sumer.getValue(key);
		Datum number = counter.getValue(key);		
		return new DatumFloat((float)(total.getNumericValue()/number.getNumericValue()));
	}

	@Override
	public void aggregate(Tuple tuple, String key) {
		sumer.aggregate(tuple, key);
		counter.aggregate(tuple, key);
	}

}
