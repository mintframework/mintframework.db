package mint.db;

import java.util.HashMap;

public class ResultMap extends HashMap<String, String> {
	private static final long serialVersionUID = 1L;

	public Integer getInteger(String key){
		return Integer.valueOf(this.get(key));
	}
	
	public Long getLong(String key){
		return Long.valueOf(this.get(key));
	}
	
	public Float getFloat(String key){
		return Float.valueOf(this.get(key));
	}
	
	public Double getDouble(String key){
		return Double.valueOf(this.get(key));
	}
	
	public Short getChar(String key){
		return Short.valueOf(this.get(key));
	}
	
	public Boolean getBoolean(String key){
		return  Boolean.valueOf(this.get(key));
	}
}
