package mint.db;

import java.util.HashMap;

public class ResultMap extends HashMap<String, String> {
	private static final long serialVersionUID = 1L;

	public Integer getInteger(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		} else {
			return Integer.valueOf(this.get(key));
		}
	}
	
	public Long getLong(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		} else {
			return Long.valueOf(this.get(key));
		}
	}
	
	public Float getFloat(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		} else {
			return Float.valueOf(this.get(key));
		}
	}
	
	public Double getDouble(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		} else {
			return Double.valueOf(this.get(key));
		}
	}
	
	public Short getChar(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		} else {
			return Short.valueOf(this.get(key));
		}
	}
	
	public Boolean getBoolean(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		} else {
			return  Boolean.valueOf(this.get(key));
		}
	}
}
