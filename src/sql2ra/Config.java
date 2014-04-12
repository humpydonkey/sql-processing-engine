package sql2ra;

public class Config {
	public static final int Buffer_SIZE = 10000;	//10w row ¡Ö 10~20MB
	public static final long OneMB = 1048576;	//1024*1024
	
	/**
	 * If a file's size>Threshold_MB, then write into disk
	 */
	public static final long FileThreshold_MB = 10*OneMB;	//MB
	
	
	public static final boolean PrintRuningTime = false;
}
