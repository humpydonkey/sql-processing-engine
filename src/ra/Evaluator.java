package ra;

import java.util.Date;

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
import dao.Datum;
import dao.DatumDate;
import dao.DatumFloat;
import dao.DatumLong;
import dao.DatumString;
import dao.Tuple;

public class Evaluator implements ExpressionVisitor{

	private Tuple tuple;
	private boolean evalResult;
	private Datum data;
	
	public Evaluator(Tuple tupleIn){
		evalResult = true;
		tuple = tupleIn;
	}
	
	
	public boolean getResult(){	
		return evalResult;
	}
	
	public Datum getDatum(){
		return data;
	}
	
	
	@Override
	public void visit(NullValue arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Function arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
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
		data = new DatumFloat((float)arg.getValue());
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
		Datum left = data;
		
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		data.setNumericValue(left.getNumericValue() + right.getNumericValue());
	}

	@Override
	public void visit(Division arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		data.setNumericValue(left.getNumericValue() / right.getNumericValue());
	}

	@Override
	public void visit(Multiplication arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		data.setNumericValue(left.getNumericValue() * right.getNumericValue());
	}

	@Override
	public void visit(Subtraction arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		
		arg.getRightExpression().accept(this);
		Datum right = data;
		
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
	DatumString colName = (DatumString)data;
	
		Datum var = tuple.getDataByName(colName.getValue());
		if(var==null)
			throw new NullPointerException();
		
		arg.getBetweenExpressionStart().accept(this);
		Datum start = data;
		arg.getBetweenExpressionEnd().accept(this);
		Datum end = data;
		
		if(var instanceof DatumDate){
			DatumDate dateData = (DatumDate)var;
			Date date = dateData.getValue();

			DatumDate startDatum = (DatumDate)start;
			Date startDate = startDatum.getValue();
			
			DatumDate endDatum = (DatumDate)end;
			Date endDate = endDatum.getValue();
			
			if(date.compareTo(startDate)>0&&date.compareTo(endDate)<0){
				evalResult = true;
			}else
				evalResult = false;
			
		}else{
			double varVal = var.getNumericValue();				
			double startVal= start.getNumericValue();
			double endVal = end.getNumericValue();
			
			if(varVal>startVal&&varVal<endVal)
				evalResult = true;
			else
				evalResult = false;
		}
		
	}

	@Override
	public void visit(EqualsTo arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		if(left instanceof DatumString){
			DatumString leftVar = (DatumString)left;

			if(right instanceof DatumString){
				//two variables
				DatumString rightVar = (DatumString)right;
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					Datum rightDatum = tuple.getDataByName(rightVar.getValue());
					if(leftDatum==null||rightDatum==null)
						throw new NullPointerException();
					evalResult = Datum.equals(leftDatum, rightDatum);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				//one variable, one constant
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					evalResult = Datum.equals(leftDatum, right);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else{
			//two constant
			try {
				evalResult = Datum.equals(left, right);
				return;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}
	}

	
	@Override
	public void visit(GreaterThan arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		if(left instanceof DatumString){
			DatumString leftVar = (DatumString)left;

			if(right instanceof DatumString){
				//two variables
				DatumString rightVar = (DatumString)right;
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					Datum rightDatum = tuple.getDataByName(rightVar.getValue());
					if(leftDatum==null||rightDatum==null)
						throw new NullPointerException();
					evalResult = (Datum.compare(leftDatum, rightDatum)>0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				//one variable, one constant
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					evalResult = (Datum.compare(leftDatum, right)>0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else{
			//two constant
			try {
				evalResult = (Datum.compare(left, right)>0);
				return;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}
	}

	@Override
	public void visit(GreaterThanEquals arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		if(left instanceof DatumString){
			DatumString leftVar = (DatumString)left;

			if(right instanceof DatumString){
				//two variables
				DatumString rightVar = (DatumString)right;
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					Datum rightDatum = tuple.getDataByName(rightVar.getValue());
					if(leftDatum==null||rightDatum==null)
						throw new NullPointerException();
					evalResult = (Datum.compare(leftDatum, rightDatum)>=0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				//one variable, one constant
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					evalResult = (Datum.compare(leftDatum, right)>=0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else{
			//two constant
			try {
				evalResult = (Datum.compare(left, right)>=0);
				return;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		} 
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
		Datum left = data;
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		if(left instanceof DatumString){
			DatumString leftVar = (DatumString)left;

			if(right instanceof DatumString){
				//two variables
				DatumString rightVar = (DatumString)right;
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					Datum rightDatum = tuple.getDataByName(rightVar.getValue());
					if(leftDatum==null||rightDatum==null)
						throw new NullPointerException();
					evalResult = (Datum.compare(leftDatum, rightDatum)<0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				//one variable, one constant
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					evalResult = (Datum.compare(leftDatum, right)<0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else{
			//two constant
			try {
				evalResult = (Datum.compare(left, right)<0);
				return;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}
	}

	@Override
	public void visit(MinorThanEquals arg) {
		arg.getLeftExpression().accept(this);
		Datum left = data;
		arg.getRightExpression().accept(this);
		Datum right = data;
		
		if(left instanceof DatumString){
			DatumString leftVar = (DatumString)left;

			if(right instanceof DatumString){
				//two variables
				DatumString rightVar = (DatumString)right;
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					Datum rightDatum = tuple.getDataByName(rightVar.getValue());
					if(leftDatum==null||rightDatum==null)
						throw new NullPointerException();
					evalResult = (Datum.compare(leftDatum, rightDatum)<=0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				//one variable, one constant
				try {
					Datum leftDatum = tuple.getDataByName(leftVar.getValue());
					evalResult = (Datum.compare(leftDatum, right)<=0);
					return;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else{
			//two constant
			try {
				evalResult = (Datum.compare(left, right)<=0);
				return;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}
	}

	@Override
	public void visit(NotEqualsTo arg) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public void visit(Column arg) {
		Datum var = tuple.getDataByName(arg.getColumnName());
		if(var==null)
			throw new NullPointerException();
		data = var;
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
