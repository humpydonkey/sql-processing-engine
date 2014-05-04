package ra;

import dao.Datum;
import dao.Datum.CastError;
import dao.DatumDouble;
import dao.Tuple;
import net.sf.jsqlparser.expression.Function;

/**
 * Aggregate average function
 * @author Asia
 *
 */
public class AggregatorAvg extends Aggregator{

	private Aggregator sumer;
	private Aggregator counter;
	
	public AggregatorAvg(Function funcIn, String[] groupByNamesIn) {
		super(funcIn, groupByNamesIn);
		sumer = new AggregatorSum(funcIn, groupByNamesIn);
		counter = new AggregatorCount(funcIn, groupByNamesIn);
	}
	
	@Override
	public Datum getValue(String key) throws CastError {
		Datum total = sumer.getValue(key);
		Datum number = counter.getValue(key);		
		return new DatumDouble(total.toDouble()/number.toDouble());
	}

	@Override
	public void aggregate(Tuple tuple, String key) {
		sumer.aggregate(tuple, key);
		counter.aggregate(tuple, key);
	}

}
