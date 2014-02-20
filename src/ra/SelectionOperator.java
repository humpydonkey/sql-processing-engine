package ra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import dao.Schema;
import dao.Tuple;

public class SelectionOperator implements Operator{

	private Operator input;
	private Schema schema;
	private Expression condition;
	
	public static void main(String[] args){
		String add1 = "data/NBA/nba11.sql";
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileInputStream(new File(add1)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SelectionOperator(Operator inputIn, Schema schemaIn, Expression conditionIn){
		input = inputIn;
		schema = schemaIn;
		condition = conditionIn;
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = null;
		do{
			tuple = input.readOneTuple();
			if(tuple == null)
				return null;
			
			Evaluator evaluator = new Evaluator(schema, tuple);
			condition.accept(evaluator);
			if(!evaluator.getResult()){
				tuple = null;
			}
		
		}while(tuple == null);
		
		return tuple;
	}
	
	@Override
	public void reset() {
		input.reset();
	}
	
}
