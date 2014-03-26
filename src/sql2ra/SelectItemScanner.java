package sql2ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import ra.Aggregator;
import ra.AggregatorAvg;
import ra.AggregatorCount;
import ra.AggregatorSum;
import dao.DatumType;
import dao.Schema;

/**
 * parse selected item and generate new schema
 * constant and function will be stored as a DatumString in Column[]
 * if it is a function, then parse it and store it in colSources as a Function,
 * and generate related aggregator
 * @author Asia
 *
 */
public class SelectItemScanner implements SelectItemVisitor{
	
	private List<SelectItem> items;
	private Schema newSchema;
	private boolean ifSelectAll;
	private List<Aggregator> aggregators;
	
	private List<Column> colNames;
	private List<DatumType> colTypes;
	private List<Expression> colSources;
	private Map<Function, Aggregator> aggreMap;
	private String[] groupbyNames;
	
	@SuppressWarnings("unchecked")
	public SelectItemScanner(PlainSelect select){
		items = select.getSelectItems();
		ifSelectAll = false;
		
		//initialization
		colNames = new LinkedList<Column>();
		colTypes = new LinkedList<DatumType>();
		colSources = new LinkedList<Expression>();
		aggreMap = new HashMap<Function, Aggregator>();
		aggregators = new ArrayList<Aggregator>();
		
		//get group by column names
		List groupbys = select.getGroupByColumnReferences();
		if(groupbys!=null){
			groupbyNames = new String[groupbys.size()];
			for(int i=0; i<groupbys.size(); i++){
				Column col = (Column)groupbys.get(i);
				groupbyNames[i] = col.getColumnName();
			}	
		}
				
		for(SelectItem item : items){
			item.accept(this);
		}
			
		if(!ifSelectAll){		
			//create new Schema
			int length = colNames.size();
			Column[] colArr = new Column[length];
			DatumType[] typeArr = new DatumType[length];
			Expression[] exprArr = new Expression[length];
			colNames.toArray(colArr);
			colTypes.toArray(typeArr);
			colSources.toArray(exprArr);
			newSchema = new Schema(colArr, typeArr,exprArr, aggreMap);
		}
	}
	
	public Aggregator[] getAggregators(){
		if(aggregators==null)
			return null;
		else{
			Aggregator[] aggrArr = new Aggregator[aggregators.size()];
			return aggregators.toArray(aggrArr);
		}
	}
	
	public String[] getGroupByNames(){
		return groupbyNames;
	}
	
	public Schema getSelectedColumns(){
		return newSchema;
	}
	
	public boolean getIfSelectAll(){
		return ifSelectAll;
	}

	@Override
	public void visit(AllColumns arg0) {
		ifSelectAll = true;
	}

	@Override																																										
	public void visit(AllTableColumns arg0) {
		ifSelectAll = true;
	}

	@Override
	public void visit(SelectExpressionItem arg0) {
		String alias = arg0.getAlias();
		Expression exp = arg0.getExpression();
		
		Column col = null;
		colTypes.add(DatumType.String);
		
		if(exp instanceof Column){
			/***** original column *****/
			col =(Column)exp;
			if(alias!=null)
				col.setColumnName(alias);
			
			colNames.add(col);
			colSources.add(col);
		}else if(exp instanceof Function){
			/***** aggregate function *****/
			col = new Column();
			if(alias!=null)
				col.setColumnName(alias);
			else
				col.setColumnName(exp.toString());
			
			Function func = (Function)exp;
			colNames.add(col);
			colSources.add(func);

			//sum, count, avg, max, min
			String funcName = func.getName().toUpperCase();
			switch(funcName){
				case "AVG":
				Aggregator aggrAvg = new AggregatorAvg(func, groupbyNames);
				aggregators.add(aggrAvg);
				aggreMap.put(func, aggrAvg);
				return;
			case "COUNT":
				Aggregator aggrCount = new AggregatorCount(func, groupbyNames);
				aggregators.add(aggrCount);
				aggreMap.put(func, aggrCount);
				return;
			case "SUM":
				Aggregator aggrSum = new AggregatorSum(func, groupbyNames);
				aggregators.add(aggrSum);
				aggreMap.put(func, aggrSum);
				return;
			case "MAX":
				return;
			case "MIN":
				return;
			default:
				return;
				
			}
		}else if(exp instanceof Parenthesis){
			/***** could be a arithmetic expression *****/
			Parenthesis paren = (Parenthesis)exp; 

			col = new Column();
			if(alias!=null)
				col.setColumnName(alias);
			else
				col.setColumnName(exp.toString());
			
			colSources.add(paren);
			colNames.add(col);
		}else{
			throw new UnsupportedOperationException("Not supported yet. Class:" + exp.getClass().getCanonicalName());
		}
	}
}
