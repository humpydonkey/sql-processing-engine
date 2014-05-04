package sqlparse;

import java.io.IOException;
import java.rmi.UnexpectedException;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;
import dao.Datum;
import dao.DatumType;
import dao.Row;
import dao.Schema;
import dao.Datum.CastError;

public class IndexBuilder {

	private RecordManager manager;
	
	public IndexBuilder(String name){
		try {
			manager = RecordManagerFactory.createRecordManager(name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void build(Schema schema) throws UnexpectedException{
		if(manager==null){
			throw new UnexpectedException("RecordManager is null!");
		}
		
		//manager.hashMap(schema.getTableName(), arg1);
	}
	
	public static class RowSerializer implements Serializer<Row>{
		@Override
		public Row deserialize(SerializerInput arg0) throws IOException,
				ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void serialize(SerializerOutput output, Row row)
				throws IOException {
			for(Datum data : row.getData()){
				DatumType type = data.getType();
				try{
					switch (type) {
					case Bool:	
						output.writeBoolean(data.toBool());
						break;
					case Long:
						output.writeLong(data.toLong());
						break;
					case Double:
						output.writeDouble(data.toDouble());
						break;
					default:
						output.writeUTF(data.toString());
						break;
					}	
				}catch(CastError e){
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
