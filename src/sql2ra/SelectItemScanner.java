package sql2ra;

import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import ra.Operator;
import ra.ProjectEvaluator;
import ra.ProjectionOperator;
import dao.DatumType;
import dao.Schema;

/**
 * parse selected item and generate new schema
 * constant will be stored as a DatumString in Column[]
 * if selected item is a function, then it should calculate it at the same
 * @author Asia
 *
 */
public class SelectItemScanner implements SelectItemVisitor{
	
	private List<SelectItem> items;
	private Operator input;
	private ProjectEvaluator eval;
	private List<Column> cols;
	private List<DatumType> types;
	private boolean ifSelectAll;
	
	@SuppressWarnings("unchecked")
	public SelectItemScanner(PlainSelect select, Operator inputIn){
		items = select.getSelectItems();
		input = inputIn;
		eval = new ProjectEvaluator();
		cols = new LinkedList<Column>();
		types = new LinkedList<DatumType>();
		ifSelectAll = false;
		
		for(SelectItem item : items){
			//parsing if it is a SelectExpressionItem
			item.accept(this);
		}
	}

	
	public Operator getOutput(){
		if(ifSelectAll)
			return input;	
		
		Column[] colArr = new Column[cols.size()];
		DatumType[] typeArr = new DatumType[types.size()];
		cols.toArray(colArr);
		types.toArray(typeArr);
		
		Schema newSchema = new Schema(colArr, typeArr);
		return new ProjectionOperator(input, newSchema);
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
		Expression exp = arg0.getExpression();	
		exp.accept(eval);
		String selectedColumn = eval.getColName();
		cols.add(new Column(null, selectedColumn));
		types.add(DatumType.String);
	}
}
