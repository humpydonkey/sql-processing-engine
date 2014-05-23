package sqlparse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Parse Table from FromItem
 * Visitor pattern
 * if a FromItem is a Table,
 * then put it into fromItemTable
 * @author Asia
 *
 */
public class TableParser implements FromItemVisitor {
	
	private Map<String, Table> fromItemTable;
	
	public TableParser(PlainSelect pselect){
		fromItemTable = new HashMap<String, Table>();
		pselect.getFromItem().accept(this);
		@SuppressWarnings("unchecked")
		List<Join> joinList = pselect.getJoins();
		if(joinList!=null){
			for(Join t : joinList){
				Table table = (Table)t.getRightItem();
				table.accept(this);	//generate scan operator
			}
		}
	}

	public Map<String, Table> getFromItemTable() {
		return fromItemTable;
	}

	
	@Override
	public void visit(Table arg0) {
		String tname = arg0.getAlias();
		if(tname==null){
			tname = arg0.getName();
		}

		getFromItemTable().put(tname, arg0);
	}

	@Override
	public void visit(SubSelect arg0) {}

	@Override
	public void visit(SubJoin arg0) {}


}
