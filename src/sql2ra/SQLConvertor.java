package sql2ra;


public class SQLConvertor {
	
	private static SQLConvertor instance = new SQLConvertor();
	
	private SQLConvertor(){}
	
	public SQLConvertor getInstance(){
		return instance;
	}
	
	public static void main(String[] args){
		System.out.println("start");
		String sql = "";
		
		SQLConvertor.instance.selectConvert(sql);
	}
	
	public void selectConvert(String sql){
		
	}
}
