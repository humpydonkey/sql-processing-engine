package ra;

import java.io.BufferedWriter;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import dao.Datum;
import dao.DatumType;
import dao.Schema;
import dao.Tuple;

public abstract class OperatorHashJoin implements Operator {
	private Schema joinedSchema;
	
	@Override
	public Schema getSchema() {
		return joinedSchema;
	}
	
	
	protected void joinAndWrite(String attr, Tuple data, Map<String,List<Tuple>> map, BufferedWriter writer) throws IOException{
		Datum keyData = data.getDataByName(attr);
		if(keyData==null){
			try {
				throw new UnexpectedException("Can't get data from tuple : " + data.toString());
			} catch (UnexpectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		String key = keyData.toString();
		List<Tuple> matches = map.get(key);
		if(matches!=null){
			for(Tuple match : matches){
				Tuple result = joinTuple(data, match, attr);
				writer.write(result.toString());
				writer.newLine();
			}
		}
	}
	
	protected void joinAndBuffer(String attr, Tuple data, Map<String,List<Tuple>> map,List<Tuple> buffer){
		Datum keyData = data.getDataByName(attr);
		if(keyData==null){
			try {
				throw new UnexpectedException("Can't get data from tuple : " + data.toString());
			} catch (UnexpectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		String key = keyData.toString();
		List<Tuple> matches = map.get(key);
		if(matches!=null){
			for(Tuple match : matches){
				Tuple result = joinTuple(data, match, attr);
				buffer.add(result);
			}
		}
	}
	
	
	
	protected void addTuple(String attr, Tuple tuple, Map<String, List<Tuple>> map){
		Datum keyData = tuple.getDataByName(attr);
		if(keyData==null){
			try {
				throw new UnexpectedException("Can't get data from tuple : " + tuple.toString());
			} catch (UnexpectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		String key = keyData.toString();
		List<Tuple> matchList = map.get(key);
		if(matchList==null){
			matchList = new LinkedList<Tuple>();
			matchList.add(tuple);
			map.put(key, matchList);
		}else
			matchList.add(tuple);
	}
	
	
	public Tuple joinTuple(Tuple t1, Tuple t2, String equalColNameIn){
		if(joinedSchema==null){
			joinedSchema = joinSchema(equalColNameIn, t1.getSchema(), t2.getSchema());
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
	
	public Schema joinSchema(String equalColNameIn, Schema s1, Schema s2){
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
		
		int equalColIndex = s1.getColIndex(equalColNameIn);
		int j=0;
		for(int i=0; i<colNames1.length; i++){
			joinedNames[j] = colNames1[i];
			joinedSources[j] = colSources1[i];
			joinedTypes[j] = colTypes1[i];
			j++;
		}
		
		equalColIndex = s2.getColIndex(equalColNameIn);
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
}
