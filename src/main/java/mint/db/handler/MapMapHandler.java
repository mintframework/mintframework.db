package mint.db.handler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import mint.db.ResultSetHandler;

/** 
 * bean处理器。将查询结果集的第一行封装成指定类型的bean后返回。
 * 指定的bean需要符合bean规范，对应的属性要写上getter 和 setter
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:43:01 
 * 
 * @param <T> 
 */
public class MapMapHandler implements ResultSetHandler<Map<String, Map<String, String>>>{
	private String keyColumn;
	
	public MapMapHandler(final String keyColumn){
		if(keyColumn==null || "".equals(keyColumn)){
			throw new RuntimeException(keyColumn+"can not be empty");
		}
		this.keyColumn = keyColumn;
	}
	
	@Override
	public Map<String, Map<String, String>> handle(ResultSet result) throws SQLException {
		if(result.next()){
			ResultSetMetaData meta = result.getMetaData();
			//是否有被声明的字段
			for(int i=1,j=meta.getColumnCount(); i<=j; i++){
				if(meta.getColumnName(i).equals(keyColumn)){
					break;
				} else if(i==j){
					throw new RuntimeException("there is no column named "+keyColumn);
				}
			}
			
			Map<String, Map<String, String>> mapMap = new HashMap<String, Map<String, String>>();
			String key;
			do {
				Map<String, String> map = new HashMap<String, String>();
				for(int i=1,j=meta.getColumnCount(); i<=j; i++){
					key = meta.getColumnName(i);
					map.put(key, result.getString(i));
					if(key.equals(keyColumn)){
						mapMap.put(result.getString(i), map);
					}
				}
				
			} while(result.next());
			
			return mapMap;
		}
		
		return null;
	}
}
