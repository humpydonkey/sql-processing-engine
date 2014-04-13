package ra;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;
import dao.CompareAttribute;
import dao.Schema;
import dao.Tuple;

public class OperatorOrderBy{
	
	private CompareAttribute[] compAttrs;
	
	public OperatorOrderBy(List<Tuple> listIn, List<OrderByElement> eles) throws Exception{
	
		compAttrs = new CompareAttribute[eles.size()];
		
		for(int i=0; i<eles.size(); i++){
			OrderByElement obe = eles.get(i);
			Expression expr = obe.getExpression();
			if(expr instanceof Column){
				Column col = (Column)expr;
				compAttrs[i] = new CompareAttribute(col, obe.isAsc());
			}else
				throw new UnsupportedOperationException("Unsupported, OrderByElement is an expression! "+expr.toString());
		}
	}
	
	public CompareAttribute[] getCompAttrs(){
		return compAttrs;
	}
	
//	private void quickSort_Desc(List<Tuple> list, int left, int right) {
//		if (left >= right) {
//			return;
//		}
//
//		Datum pivot = null;
//		int center = left;
//		for (int j = left + 1; j <= right; j++) {
//			pivot = list.get(left).getDataByName(firstColName);
//			Datum movingData = list.get(j).getDataByName(firstColName);
//			int compResult = movingData.compareTo(pivot);
//			if (compResult > 0) {
//				center++;
//				if (center != j)
//					swap(list, center, j);
//			}else if(colNames.size()>1&&compResult == 0){
//				pivot = list.get(left).getDataByName(colNames.get(1));
//				movingData = list.get(j).getDataByName(colNames.get(1));
//				compResult = movingData.compareTo(pivot);
//
//				if (compResult>0 && !orderbyElements.get(1).isAsc()) {
//					center++;
//					if (center != j)
//						swap(list, center, j);
//				}
//			}
//		}
//		swap(list, center, left); // swap pivot to center
//		quickSort_Desc(list, left, center - 1); // left part
//		quickSort_Desc(list, center + 1, right); // right part
//	}
//
//	private void quickSort_Asce(List<Tuple> list, int left, int right) {
//		if (left >= right) {
//			return;
//		}
//
//		Datum pivot = null;
//		int center = left;
//		for (int j = left + 1; j <= right; j++) {
//			pivot = list.get(left).getDataByName(firstColName);
//			Datum movingData = list.get(j).getDataByName(firstColName);
//			int compResult = movingData.compareTo(pivot);
//			if (compResult < 0) {
//				center++;
//				if (center != j)
//					swap(list, center, j);
//			}else if(colNames.size()>1&&compResult == 0){
//				pivot = list.get(left).getDataByName(colNames.get(1));
//				movingData = list.get(j).getDataByName(colNames.get(1));
//				compResult = movingData.compareTo(pivot);
//
//				if (compResult<0 && orderbyElements.get(1).isAsc()) {
//					center++;
//					if (center != j)
//						swap(list, center, j);
//				}
//			}
//		}
//		swap(list, center, left); // swap pivot to center
//		quickSort_Asce(list, left, center - 1); // left part
//		quickSort_Asce(list, center + 1, right); // right part
//	}

	private void swap(List<Tuple> a, int i, int j) {
		Tuple temp = a.get(i);
		a.set(i, a.get(j));
		a.set(j, temp);
	}
	
	 
	public static void main(String[] args) throws Exception{
		
		try {
			Table tab = new Table(null,"test");
			Column col1 = new Column(tab, "A");
			Column col2 = new Column(tab, "Num");
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
			Schema schema = new Schema(new Column[]{col1,col2}, new ColumnDefinition[]{colDef1, colDef2}, new int[]{0,1});
			Tuple t1 = new Tuple(new String("C|22"), schema);
			Tuple t2 = new Tuple(new String("C|5"), schema);
			Tuple t3 = new Tuple(new String("A|11"), schema);
			Tuple t4 = new Tuple(new String("C|8"), schema);
			Tuple t5 = new Tuple(new String("B|1"), schema);
			Tuple t6 = new Tuple(new String("B|4"), schema);
			List<Tuple> tuples = new ArrayList<Tuple>();
			tuples.add(t1);
			tuples.add(t2);
			tuples.add(t3);
			tuples.add(t4);
			tuples.add(t5);
			tuples.add(t6);
			
			//order by
			List<OrderByElement> eles = new ArrayList<OrderByElement>();
			OrderByElement ele1 = new OrderByElement();
			OrderByElement ele2 = new OrderByElement();
			ele1.setExpression(col1);
			ele2.setExpression(col2);
			ele2.setAsc(true);
			eles.add(ele1);
			eles.add(ele2);
			
			OperatorOrderBy ob = new OperatorOrderBy(tuples, eles);
			Collections.sort(tuples, Tuple.getComparator(ob.getCompAttrs()));
			for(Tuple t : tuples){
				t.printTuple();
			}
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

