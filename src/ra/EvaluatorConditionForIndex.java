package ra;

import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

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
import net.sf.jsqlparser.statement.select.SubSelect;
import ra.OperatorIndexScan.IndexScanType;
import dao.Datum;
import dao.DatumDate;
import dao.DatumDouble;
import dao.DatumFactory;
import dao.DatumLong;
import dao.DatumString;
import dao.DatumType;

public class EvaluatorConditionForIndex implements ExpressionVisitor {
	public static boolean PrintDebug=false;
	private List<Datum> data;
	private Column column;
	private int priority;
	private IndexScanType type;

//priority type
//	0		All, 
//	1		NotEqualsTo,
//	2		GreaterThanEquals, MinorThanEquals
//	3		GreaterThan, MinorThan
//	4		MinorGreaterThan, 	// A < X < B
//	4		MinorThanGreaterEquals, 	// A < X <= B
//	4		MinorEqualsGreaterThan, 	// A <= X < B
//	4		MinorGreaterBothEquals, 	// A <= X <= B
//	5		EqualsTo
	
	public EvaluatorConditionForIndex(){
		priority = 0;
		data = new ArrayList<Datum>(2);
	}
	
	public Datum[] getData(){
		if(data.size()==0)
			return null;
		Datum[] d = new Datum[data.size()];
		data.toArray(d);
		data = new ArrayList<Datum>(2);	
		return d;
	}
	
	public Column getColumn(){	
		Column col = column;
		column = null;
		return col;
	}
	
	public int getPriority(){
		int t = priority;
		priority = 0;
		return t;
	}
	
	public IndexScanType getType(){
		IndexScanType t = type;
		type = IndexScanType.All;
		return t;
	}

	@Override
	public void visit(NullValue arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Function arg) {
		if(arg.getName().equalsIgnoreCase("DATE")){
			String para = arg.getParameters().getExpressions().get(0).toString();
			String date = para.substring(1, para.length()-1);
			data.add(DatumFactory.create(date, DatumType.Date));
		}
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
		data.add(new DatumDouble(arg.getValue()));
	}

	@Override
	public void visit(LongValue arg) {
		data.add(new DatumLong(arg.getValue()));
	}

	@SuppressWarnings("deprecation")
	@Override
	public void visit(DateValue arg) {
		Date date = arg.getValue();
		data.add( new DatumDate(date.getYear(), date.getMonth(), date.getDay()));
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
		data.add(new DatumString(arg.getValue()));
	}

	@Override
	public void visit(Addition arg) {}

	@Override
	public void visit(Division arg) {}

	@Override
	public void visit(Multiplication arg) {}

	@Override
	public void visit(Subtraction arg) {}

	@Override	//merge two conditions
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(this);
		Column lcol = column;
		IndexScanType ltype = type;
		if(ltype==IndexScanType.All||ltype==IndexScanType.EqualsTo||ltype==IndexScanType.NotEqualsTo){
			return;
		}
		
		arg.getRightExpression().accept(this);
		Column rcol = column;
		IndexScanType rtype = getType();
		if(rtype==IndexScanType.All||rtype==IndexScanType.EqualsTo||rtype==IndexScanType.NotEqualsTo){
			return;
		}
		
		if(lcol.getColumnName().equals(rcol.getColumnName())){			
			if((ltype==IndexScanType.GreaterThan&&rtype==IndexScanType.MinorThan)||(rtype==IndexScanType.GreaterThan&&ltype==IndexScanType.MinorThan)){
				type = IndexScanType.MinorGreaterThan;
			}else if((ltype==IndexScanType.GreaterThanEquals&&rtype==IndexScanType.MinorThan)||(rtype==IndexScanType.GreaterThanEquals&&ltype==IndexScanType.MinorThan)){
				type = IndexScanType.MinorEqualsGreaterThan;
			}else if((ltype==IndexScanType.GreaterThan&&rtype==IndexScanType.MinorThanEquals)||(rtype==IndexScanType.GreaterThan&&ltype==IndexScanType.MinorThanEquals)){
				type = IndexScanType.MinorThanGreaterEquals;
			}else
				type = IndexScanType.MinorGreaterBothEquals;
			
			Collections.sort(data);
			
		}else{
			try {
				throw new UnexpectedException("The two column must be the same!");
			} catch (UnexpectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void visit(OrExpression arg) {}

	/*
	 * The range of start and end include equal value
	 * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.Between)
	 */
	@Override
	public void visit(Between arg) {
		arg.getLeftExpression().accept(this);
		
		arg.getBetweenExpressionStart().accept(this);
		arg.getBetweenExpressionEnd().accept(this);
		type = IndexScanType.MinorGreaterThan;
		priority = 4;
	}

	@Override
	public void visit(EqualsTo arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
		type = IndexScanType.EqualsTo;
		priority = 5;
	}

	
	@Override
	public void visit(GreaterThan arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
		type = IndexScanType.GreaterThan;
		priority = 3;
	}

	@Override
	public void visit(GreaterThanEquals arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
		type = IndexScanType.GreaterThanEquals;
		priority = 2;
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
	}

	@Override
	public void visit(MinorThan arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
		type = IndexScanType.MinorThan;
		priority = 3;
	}

	@Override
	public void visit(MinorThanEquals arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
		type = IndexScanType.MinorThanEquals;
		priority = 2;
	}

	@Override
	public void visit(NotEqualsTo arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
		type = IndexScanType.NotEqualsTo;
		priority = 1;
	}

	@Override
	public void visit(Column arg) {
		column = arg;
	}
	

	@Override
	public void visit(SubSelect subselect) {}

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
