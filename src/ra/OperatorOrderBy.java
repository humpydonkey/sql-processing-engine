package ra;

import java.io.FileNotFoundException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;
import dao.Datum;
import dao.Schema;
import dao.Tuple;

public class OperatorOrderBy{

	private List<Tuple> results;
	private List<OrderByElement> orderbyInfo;
	private List<String> colNames;	//ordered column names
	private String firstColName;
	
	public OperatorOrderBy(List<Tuple> listIn, List<OrderByElement> eles) throws Exception{
		results = listIn;		
		orderbyInfo = eles;
		colNames = new ArrayList<String>(eles.size());
		
		for(OrderByElement ele : eles){
			Expression expr = ele.getExpression();
			if(expr instanceof Column){
				Column col = (Column)expr;
				colNames.add(col.getColumnName());
			}else
				throw new UnsupportedOperationException("Unsupported, OrderByElement is an expression! "+expr.toString());
		}
		
		if(colNames.size()==0)
			throw new UnexpectedException("The size of OrderBy ColNames should not be 0");
		
		firstColName = colNames.get(0);
		
		if(orderbyInfo.get(0).isAsc()){
			quickSort_Asce(results, 0, results.size()-1);
		}else{
			quickSort_Desc(results, 0, results.size()-1);
		}
	}
	
	public List<Tuple> getResults(){
		return results;
	}

	
	private void quickSort_Desc(List<Tuple> list, int left, int right) {
		if (left >= right) {
			return;
		}

		Datum pivot = list.get(left).getDataByName(firstColName);
		int center = left;
		for (int j = left + 1; j <= right; j++) {
			Datum movingData = list.get(j).getDataByName(firstColName);
			int compResult = movingData.compareTo(pivot);
			if (compResult > 0) {
				center++;
				if (center != j)
					swap(list, center, j);
			}else if(colNames.size()>1&&compResult == 0){
				pivot = list.get(left).getDataByName(colNames.get(1));
				movingData = list.get(j).getDataByName(colNames.get(1));
				compResult = movingData.compareTo(pivot);

				if (compResult>0 && !orderbyInfo.get(1).isAsc()) {
					center++;
					if (center != j)
						swap(list, center, j);
				}
			}
		}
		swap(list, center, left); // swap pivot to center
		quickSort_Desc(list, left, center - 1); // left part
		quickSort_Desc(list, center + 1, right); // right part
	}

	private void quickSort_Asce(List<Tuple> list, int left, int right) {
		if (left >= right) {
			return;
		}

		Datum pivot;
		int center = left;
		for (int j = left + 1; j <= right; j++) {
			pivot = list.get(left).getDataByName(firstColName);
			Datum movingData = list.get(j).getDataByName(firstColName);
			int compResult = movingData.compareTo(pivot);
			if (compResult < 0) {
				center++;
				if (center != j)
					swap(list, center, j);
			}else if(colNames.size()>1&&compResult == 0){
				pivot = list.get(left).getDataByName(colNames.get(1));
				movingData = list.get(j).getDataByName(colNames.get(1));
				compResult = movingData.compareTo(pivot);

				if (compResult<0 && orderbyInfo.get(1).isAsc()) {
					center++;
					if (center != j)
						swap(list, center, j);
				}
			}
		}
		swap(list, center, left); // swap pivot to center
		quickSort_Asce(list, left, center - 1); // left part
		quickSort_Asce(list, center + 1, right); // right part
	}

	private void swap(List<Tuple> a, int i, int j) {
		Tuple temp = a.get(i);
		a.set(i, a.get(j));
		a.set(j, temp);
	}
	
	 
	public static void main(String[] args) throws Exception{
		
		try {
			Column col1 = new Column();
			Column col2 = new Column();
			col1.setColumnName("A");
			col2.setColumnName("Num");
			ColumnDefinition colDef1 = new ColumnDefinition();
			ColumnDefinition colDef2 = new ColumnDefinition();
			colDef1.setColumnName(col1.getColumnName());
			colDef2.setColumnName(col2.getColumnName());
			ColDataType type1 = new ColDataType();
			ColDataType type2 = new ColDataType();
			type1.setDataType("string");
			type2.setDataType("int");
			colDef1.setColDataType(type1);
			colDef2.setColDataType(type2);
			List<ColumnDefinition> defs =  new ArrayList<ColumnDefinition>();
			defs.add(colDef1);
			defs.add(colDef2);
			Schema schema = new Schema(new Column[]{col1,col2},defs);
			Tuple t1 = new Tuple(new String[]{"E","22"}, schema);
			Tuple t2 = new Tuple(new String[]{"F","5"}, schema);
			Tuple t3 = new Tuple(new String[]{"A","11"}, schema);
			Tuple t4 = new Tuple(new String[]{"C","8"}, schema);
			Tuple t5 = new Tuple(new String[]{"B","1"}, schema);
			Tuple t6 = new Tuple(new String[]{"B","4"}, schema);
			List<Tuple> tuples = new ArrayList<Tuple>();
			tuples.add(t1);
			tuples.add(t2);
			tuples.add(t3);
			tuples.add(t4);
			tuples.add(t5);
			tuples.add(t6);
			List<OrderByElement> eles = new ArrayList<OrderByElement>();
			OrderByElement ele1 = new OrderByElement();
			OrderByElement ele2 = new OrderByElement();
			ele1.setExpression(col1);
			ele2.setExpression(col2);
			ele2.setAsc(false);
			eles.add(ele1);
			eles.add(ele2);
			OperatorOrderBy ob = new OperatorOrderBy(tuples, eles);

			for(Tuple t : ob.getResults())
				t.printTuple();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

