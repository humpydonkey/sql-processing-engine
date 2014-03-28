package ra;

import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;
import dao.Datum;
import dao.DatumDate;
import dao.DatumDouble;
import dao.DatumLong;
import dao.DatumString;
import dao.DatumType;
import dao.Tuple;

//it will ignore the comparison of a column from a different table
//and ignore equal join condition
public class EvaluatorConditionExpres implements ExpressionVisitor{

	private Function func;
	private Tuple tuple;
	private boolean evalResult;
	private Datum data;
	private Column column;
	private boolean differentTable;


	public EvaluatorConditionExpres(Tuple tupleIn){
		evalResult = true;
		tuple = tupleIn;
		differentTable = false;
	}
	
	
	public boolean getResult(){	
		return evalResult;
	}

	
	public Datum getData(){
		Datum d = data;
		data = null;
		
		if(d==null)
			return null;
		else
			return d.clone();
	}
	
	public Column getColumn(){	
		Column col = column;
		column = null;
		return col;
	}
	
	public Function getFunc(){
		if(func==null){
			try {
				throw new Exception("Expression analysis error!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		Function f = func;
		func = null;
		return f;
	}


	@Override
	public void visit(NullValue arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Function arg) {
		if(arg.getName().equalsIgnoreCase("DATE")){
			String date = arg.getParameters().toString();
			String tmp = date.substring(2, date.length()-2);
			data = new DatumDate(tmp);
		}else
			func = arg;
	}

	@Override
	public void visit(InverseExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(JdbcParameter arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(DoubleValue arg) {
		data = new DatumDouble(arg.getValue());
	}

	@Override
	public void visit(LongValue arg) {
		data = new DatumLong(arg.getValue());
	}

	@Override
	public void visit(DateValue arg) {
		data = new DatumDate(arg.getValue());
	}

	@Override
	public void visit(TimeValue arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(TimestampValue arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Parenthesis arg) {
		arg.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg) {
		data = new DatumString(arg.getValue());
	}

	@Override
	public void visit(Addition arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		if(left.getType()==DatumType.Long&&right.getType()==DatumType.Long)
			data = new DatumLong(0);
		else
			data = new DatumDouble(0);
		
		data.setNumericValue(left.getNumericValue() + right.getNumericValue());
	}

	@Override
	public void visit(Division arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		if(left.getType()==DatumType.Long&&right.getType()==DatumType.Long)
			data = new DatumLong(0);
		else
			data = new DatumDouble(0);
		
		if(right.getNumericValue()==0)
			data.setNumericValue(0);
		else
			data.setNumericValue(left.getNumericValue() / right.getNumericValue());
	}

	@Override
	public void visit(Multiplication arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();
	
	
		if(left.getType()==DatumType.Long&&right.getType()==DatumType.Long)
			data = new DatumLong(0);
		else
			data = new DatumDouble(0);
		
		data.setNumericValue(left.getNumericValue() * right.getNumericValue());
	}

	@Override
	public void visit(Subtraction arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		if(left.getType()==DatumType.Long&&right.getType()==DatumType.Long)
			data = new DatumLong(0);
		else
			data = new DatumDouble(0);
		
		data.setNumericValue(left.getNumericValue() - right.getNumericValue());
	}

	@Override
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(this);
		boolean left = evalResult;
		
		arg.getRightExpression().accept(this);
		boolean right = evalResult;
		
		evalResult = (left&&right);
	}

	@Override
	public void visit(OrExpression arg) {
		arg.getLeftExpression().accept(this);
		boolean left = evalResult;
		arg.getRightExpression().accept(this);
		boolean right = evalResult;
		evalResult = (left||right);
	}

	@Override
	public void visit(Between arg) {
		arg.getLeftExpression().accept(this);
		Column col = getColumn();
	
		Datum var = tuple.getDataByName(col.getColumnName());
		if(var==null)
			throw new NullPointerException();
		
		arg.getBetweenExpressionStart().accept(this);
		//Datum start = getData();
		arg.getBetweenExpressionEnd().accept(this);
		//Datum end = getData();
		
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(EqualsTo arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		Column leftCol = getColumn();
		arg.getRightExpression().accept(this);
		Datum right = getData();
		Column rightCol = getColumn();	
		
		if(differentTable){//mask
			//ignore comparison of columns of different table
			evalResult = true;
			differentTable = false;
			return;
		}
		
		//if they have same column name
		if(leftCol!=null&&rightCol!=null){
			if(leftCol.getColumnName().equalsIgnoreCase(rightCol.getColumnName())){
				//it is a equal join, ignore it
				differentTable = true;
				return;
			}
		}else{	//common equals expression			
			int compResult = left.compareTo(right);
			if(compResult==0)
				evalResult = true;
			else
				evalResult = false;
		}
	}

	
	@Override
	public void visit(GreaterThan arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		if(differentTable){//mask
			//ignore comparison of columns of different table
			evalResult = true;
			differentTable = false;
			return;
		}
		
		
		int compResult = left.compareTo(right);
		if(compResult>0)
			evalResult = true;
		else
			evalResult = false;
	}

	@Override
	public void visit(GreaterThanEquals arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		if(differentTable){//mask
			//ignore comparison of columns of different table
			evalResult = true;
			differentTable = false;
			return;
		}
		
		
		int compResult = left.compareTo(right);
		if(compResult>=0)
			evalResult = true;
		else
			evalResult = false;
	}

	@Override
	public void visit(InExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(IsNullExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(LikeExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(MinorThan arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		if(differentTable){//mask
			//ignore comparison of columns of different table
			evalResult = true;
			differentTable = false;
			return;
		}
		
		
		int compResult = left.compareTo(right);
		if(compResult<0)
			evalResult = true;
		else
			evalResult = false;
	}

	@Override
	public void visit(MinorThanEquals arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		
		if(differentTable){//mask
			//ignore comparison of columns of different table
			evalResult = true;
			differentTable = false;
			return;
		}
		
		
		int compResult = left.compareTo(right);
		if(compResult<=0)
			evalResult = true;
		else
			evalResult = false;
	}

	@Override
	public void visit(NotEqualsTo arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		
		if(differentTable){//mask
			//ignore comparison of columns of different table
			evalResult = true;
			differentTable = false;
			return;
		}
		
		
		int compResult = left.compareTo(right);
		if(compResult!=0)
			evalResult = true;
		else
			evalResult = false; 
	}

	@Override
	public void visit(Column arg) {
		column = arg;
		if(tuple==null){
			data = new DatumString(arg.getColumnName());
			return;
		}else{
			Table argTable = arg.getTable();
			if(argTable!=null){
				String argTableName = arg.getTable().toString();
				String tupTableName = tuple.getTableName();
				
				if((tupTableName!=null) && (!argTableName.equals("null")) &&(!argTableName.equalsIgnoreCase(tupTableName))){
					//in order to mask the where condition includes columns from different tables 
					//Column arg is not the from the tuple's table, just ignore it
					differentTable = true;
					return;
				}
			}
			
			Datum var = tuple.getDataByName(arg.getColumnName());
			if(var==null){
				StringBuilder sb = new StringBuilder();
				Map<String, Integer> schemaMap = tuple.getSchema().getIndexMap();
				for(Entry<String, Integer> entry : schemaMap.entrySet())
					sb.append(entry.getKey() + " | ");
				
				throw new NullPointerException("ColumnName : " +arg.getColumnName() + "\n" + sb.toString());
			}
				
			data = var;
		}
	}

	@Override
	public void visit(SubSelect arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(CaseExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(WhenClause arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(ExistsExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(AllComparisonExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(AnyComparisonExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Concat arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Matches arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(BitwiseAnd arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(BitwiseOr arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(BitwiseXor arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}
}
