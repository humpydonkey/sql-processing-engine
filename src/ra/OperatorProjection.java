package ra;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import dao.Datum;
import dao.Schema;
import dao.Tuple;

public class OperatorProjection implements Operator{
	private Operator input;
	private Schema newSchema;
	
	public OperatorProjection(Operator inputIn, Schema schemaIn){
		input = inputIn;
		newSchema = schemaIn;
	}


	@Override
	public List<Tuple> readOneBlock() {
		List<Tuple> tuples = input.readOneBlock();
		//for(Tuple tuple : tuples)
			//tuple.changeTuple(newSchema);
		
		return tuples;
	}
	
	
	@Override
	//project only variable in readOneTuple() method
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();
		if(tuple==null)
			return null;
		
		return projection(tuple, newSchema);
	}

	
	@Override
	public void reset() {
		input.reset();
	}
	
	
	/**
	 * Change tuple by new schema
	 * @param newSchema
	 * @return
	 */
	public Tuple projection(Tuple tuple, Schema newSchema){
		int length = newSchema.getLength();
		Datum[] newDataArr = new Datum[length];
		Column[] newColNames = newSchema.getColumnNames();
		Expression[] newColSources = newSchema.getColumnSources();
		//Get new data from newSchema.source
		for(int i=0; i<length; i++){
			//Get data from old tuple
			Datum oldData = null;
			Expression newSource = newColSources[i];
			Column newName = newColNames[i]; 
			if(newSource instanceof Column){
				//is a column
				oldData = tuple.getDataByName(newName.getColumnName());
			}else if(newSource instanceof Function){ 
				//is an aggregate function
				Function func = (Function)newSource;
		
				//find the key
				Aggregator aggre = newSchema.getAggregator(func);
				StringBuilder groupbyKey = new StringBuilder("");
				for(String colName : aggre.getGroupByColumns()){
					if(colName.equals(""))	//no group by
						break;
					Datum groupbyColumn = tuple.getDataByName(colName);
					groupbyKey.append(groupbyColumn.toString());
				}
				//map to the aggregated Datum value
				oldData = aggre.getValue(groupbyKey.toString());
				
			}else{
				//should be a constant or expression
				Evaluator eval = new Evaluator(tuple);
				newSource.accept(eval);
				oldData = eval.copyDatum();
				if(oldData.getNumericValue()==1168){
					System.out.println("!!");
				}
				
			}
			newDataArr[i] = oldData;
		}
		return new Tuple(newDataArr,newSchema);
	}

}
