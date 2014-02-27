package ra;

import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import sql2ra.SQLParser;
import dao.Datum;
import dao.DatumType;
import dao.Schema;
import dao.Tuple;

public class OperatorEqualJoin implements Operator {

	private Operator input;
	private Map<String, LinkedList<Tuple>> joinMap;
	//private String hashedTableName;
	private String equalColName;
	private Schema joinedSchema;
	
	public OperatorEqualJoin(Column equalColIn, Operator hashMapSource, Operator inputIn){
		input = inputIn;
		List<Tuple> tups = SQLParser.dump(hashMapSource);

		joinMap = new HashMap<String, LinkedList<Tuple>>(tups.size());
		equalColName = equalColIn.getColumnName();
		//hashedTableName = equalColIn.getTable().getName();
		for(int i=0; i<tups.size(); i++){
			Tuple tuple = tups.get(i);
			Datum keyData = tuple.getDataByName(equalColName);
			if(keyData==null){
				try {
					throw new UnexpectedException("Can't get data from tuple : " + tuple.toString());
				} catch (UnexpectedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			String key = keyData.toString();
			LinkedList<Tuple> list = joinMap.get(key);
			if(list==null){
				list = new LinkedList<Tuple>();
				list.add(tuple);
				joinMap.put(key, list);
			}else
				list.add(tuple);
				joinMap.put(key, list);
		}
	}
	
	@Override
	public Tuple readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Tuple> readOneBlock() {
		List<Tuple> results = new LinkedList<Tuple>();
		List<Tuple> inputTups = input.readOneBlock();
		for(Tuple inputTup : inputTups){
			Datum keyData = inputTup.getDataByName(equalColName);
			String key = keyData.toString();
			if(joinMap.containsKey(key)){
				List<Tuple> matches = joinMap.get(key);
				for(Tuple matchTup : matches){
					Tuple joined = joinTuple(inputTup, matchTup, equalColName);
					results.add(joined);
				}
			}
		}
		return results;
	}
	
	public Tuple joinTuple(Tuple t1, Tuple t2, String equalColNameIn){
		if(joinedSchema==null){
			joinedSchema = joinSchema(t1.getSchema(), t2.getSchema(),equalColNameIn);
		}
		
		int length = joinedSchema.getLength();
		Datum[] newDataArr = new Datum[length];
		
		int j = 0;
		for(int i=0; i<t1.getDataArr().length; i++){
			newDataArr[j] = t1.getDataArr()[i];
			j++;
		}
		
		int equalColIndex = t2.getSchema().getColIndex(equalColNameIn);
		for(int i=0; i<t2.getDataArr().length; i++){
			if(i==equalColIndex)
				continue;
			newDataArr[j] = t2.getDataArr()[i];
			j++;
		}
		
		return new Tuple(newDataArr, joinedSchema);
	}
	
	public Schema joinSchema(Schema s1, Schema s2, String equalColNameIn){
		Column[] colNames1 = s1.getColumnNames();
		Expression[] colSources1 = s1.getColumnSources();
		DatumType[] colTypes1 = s1.getColTypes();
		
		Column[] colNames2 = s2.getColumnNames();
		Expression[] colSources2 = s2.getColumnSources();
		DatumType[] colTypes2 = s2.getColTypes();
		
		int length = s1.getLength() + s2.getLength() -1;
		Column[] joinedNames = new Column[length];
		Expression[] joinedSources = new Expression[length];
		DatumType[] joinedTypes = new DatumType[length];
		
		int j=0;
		for(int i=0; i<colNames1.length; i++){
			joinedNames[j] = colNames1[i];
			joinedSources[j] = colSources1[i];
			joinedTypes[j] = colTypes1[i];
			j++;
		}
		
		int equalColIndex = s2.getColIndex(equalColNameIn);
		for(int i=0; i<colNames2.length; i++){
			if(i==equalColIndex)
				continue;
			joinedNames[j] = colNames2[i];
			joinedSources[j] = colSources2[i];
			joinedTypes[j] = colTypes2[i];
			j++;
		}
		
		return new Schema(joinedNames, joinedTypes, joinedSources, null);
	}

	@Override
	public void reset() {
		input.reset();
	}

}
