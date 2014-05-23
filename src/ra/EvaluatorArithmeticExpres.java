package ra;

import java.sql.Date;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
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
import dao.Datum;
import dao.DatumDate;
import dao.DatumDouble;
import dao.DatumFactory;
import dao.DatumLong;
import dao.DatumString;
import dao.DatumType;
import dao.Tuple;

/**
 * Parse arithmetic expression in items of select...from 
 * Only parsing arithmetic expression or constant, 
 * expression doesn't include any aggregate function.
 * Constant will be returned as a String Datum value  
 * @author Asia
 *
 */
public class EvaluatorArithmeticExpres implements ExpressionVisitor{
	
	private Tuple tuple;
	private Datum data;


	public EvaluatorArithmeticExpres(){}

	public Datum parse(Expression exp, Tuple tup){
		tuple = tup;
		exp.accept(this);
		return getData();
	}
	
	public Datum parse(Expression exp){
		exp.accept(this);
		return getData();
	}
	
	private Datum getData(){
		Datum d = data;
		data = null;
		if(d!=null)
			return d.clone();
		else
			return null;
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
			data = DatumFactory.create(date, DatumType.Date);
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
		data = new DatumDouble(arg.getValue());
	}

	@Override
	public void visit(LongValue arg) {
		data = new DatumLong(arg.getValue());
	}

	@SuppressWarnings("deprecation")
	@Override
	public void visit(DateValue arg) {
		Date date = arg.getValue();
		data = new DatumDate(date.getYear(), date.getMonth(), date.getDay());
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
		
		data = Datum.calcDatum(left, right, 1);		
	}

	@Override
	public void visit(Division arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();
		
		data = Datum.calcDatum(left, right, 4);		
	}

	@Override
	public void visit(Multiplication arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();
	
		data = Datum.calcDatum(left, right, 3);		
	}

	@Override
	public void visit(Subtraction arg) {
		arg.getLeftExpression().accept(this);
		Datum left = getData();
		
		arg.getRightExpression().accept(this);
		Datum right = getData();

		data = Datum.calcDatum(left, right, 2);
	}

	@Override
	public void visit(AndExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(OrExpression arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Between arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(EqualsTo arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	
	@Override
	public void visit(GreaterThan arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(GreaterThanEquals arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
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
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(MinorThanEquals arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(NotEqualsTo arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Column arg) {
		if(tuple!=null)
			data = tuple.getDataByName(arg);
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
