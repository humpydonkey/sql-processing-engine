package sql2ra;

import java.io.File;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorScan;
import dao.Schema;
import dao.Tuple;


/**
 * One from item corresponds one table/subselect/subjoin
 * @author Asia
 *
 */
public class FromItemEvaluator implements FromItemVisitor{
	private final File basePath;
	private Map<String,CreateTable> tables;
	private String tableName;
	
	private Schema schema = null;
	private Operator source = null;
	
	public FromItemEvaluator(File basePath, Map<String,CreateTable> tables)
	{
		this.basePath = basePath;
		this.tables = tables;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public Schema getSchema(){
		return schema;
	}
	
	public Operator getSource(){
		return source;
	}
	
	public void visit (SubJoin subjoin)
	{
		this.tableName = subjoin.toString();
	}
	
	public void visit(SubSelect subselect)
	{
		this.tableName = subselect.toString();
		SQLParser parser = new SQLParser(basePath);
		List<Tuple> tuples = parser.select(subselect.getSelectBody());
		source = new OperatorCache(tuples); 
	}
	
	public void visit(Table tableName)
	{
		this.tableName = tableName.getName();
		CreateTable table = tables.get(tableName.getName().toUpperCase());
		if(table==null)
			try {
				throw new Exception("No such table : " + tableName.getName());
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> colDefs = table.getColumnDefinitions();
		Column[] columns = new Column[colDefs.size()];
		for(int i = 0; i < colDefs.size(); i++){
			ColumnDefinition col = (ColumnDefinition)colDefs.get(i);
			columns[i] = new Column(tableName, col.getColumnName());
		}
		
		try {
			schema = new Schema(columns, colDefs);
			source = new OperatorScan(
					new File(basePath, tableName.getName() + ".dat"),
					schema
				);
//			List<Tuple> tuples = FileAccessor.getInstance().readBlock(basePath +"/" + tableName.getName() + ".dat", schema);
//			source = new OperatorCache(tuples);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
