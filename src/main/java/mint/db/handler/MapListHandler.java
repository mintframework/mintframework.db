package mint.db.handler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public class MapListHandler implements ResultSetHandler<List<Map<String, String>>>{
	@Override
	public List<Map<String, String>> handle(ResultSet result) throws SQLException {
		if(result.next()){
			ResultSetMetaData meta = result.getMetaData();
			List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
			do {
				Map<String, String> map = new HashMap<String, String>();
				for(int i=1,j=meta.getColumnCount(); i<=j; i++){
					map.put(meta.getColumnName(i), result.getString(i));
				}
				mapList.add(map);
				
			} while(result.next());
			
			return mapList;
		}
		
		return null;
	}
}
