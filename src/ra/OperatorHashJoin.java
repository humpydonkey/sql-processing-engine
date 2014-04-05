package ra;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import dao.Datum;
import dao.DatumType;
import dao.Schema;
import dao.Tuple;

public abstract class OperatorHashJoin implements Operator {
	private Schema joinedSchema;
	
	@Override
	public Tuple readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Tuple> readOneBlock() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

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
