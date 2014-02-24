package sql2ra;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import ra.CacheOperator;
import ra.Operator;
import ra.ScanOperator;
import dao.Schema;
import dao.Tuple;



public class FromScanner implements FromItemVisitor{
	File basePath;
	HashMap<String,CreateTable> tables;
	
	public Schema schema = null;
	public Column[] columns = null;
	public Operator source = null;
	
	public FromScanner(File basePath, HashMap<String,CreateTable> tables)
	{
		this.basePath = basePath;
		this.tables = tables;
	}
	
	public void visit (SubJoin subjoin)
	{
		
	}
	
	public void visit(SubSelect subselect)
	{
		List<Tuple> tuples = SQLParser.select(subselect.getSelectBody(), SQLParser.getFromScanner());
		source = new CacheOperator(tuples); 
	}
	
	public void visit(Table tableName)
	{
		CreateTable table = tables.get(tableName.getName());
		if(table==null)
			System.out.println("No such table : " + tableName.getName());
		
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> colDefs = table.getColumnDefinitions();
		columns = new Column[colDefs.size()];
		for(int i = 0; i < colDefs.size(); i++){
			ColumnDefinition col = (ColumnDefinition)colDefs.get(i);
			columns[i] = new Column(tableName, col.getColumnName());
		}
		
		try {
			schema = new Schema(columns, colDefs);
			source = new ScanOperator(
					new File(basePath, tableName.getName() + ".dat"),
					schema
				);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
