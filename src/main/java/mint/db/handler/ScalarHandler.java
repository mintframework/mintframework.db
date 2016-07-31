package mint.db.handler;

import java.sql.ResultSet;
import java.sql.SQLException;

import mint.db.DataConverterProvider;
import mint.db.ResultSetHandler;

/** 
 * 数字处理器。实际上该处理器不单单能处理数字，还可以处理字符串，chart等类型的数据。
 * 该处理器只处理查询结果集的第一条记录的第一列，并封装成指定类型的对象返回
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:43:34 
 * 
 * @param <T> 
 */
public class ScalarHandler<T> implements ResultSetHandler<T>{
	private final Class<T> type;
	
	public ScalarHandler(Class<T> type){
		this.type = type;
	}

	@Override
	public T handle(ResultSet result) throws SQLException {
		T t = null;
		if(result.next()){			
			if (type.equals(String.class)) {
				return (T) result.getString(1);
			
			} else if (type.equals(Integer.TYPE) || type.equals(Integer.class)) {
				return (T) Integer.valueOf(result.getInt(1));
			
			} else if (type.equals(Boolean.TYPE) || type.equals(Boolean.class)) {
				return (T) Boolean.valueOf(result.getBoolean(1));
			
			} else if (type.equals(Long.TYPE) || type.equals(Long.class)) {
				return (T) Long.valueOf(result.getLong(1));
			
			} else if (type.equals(Double.TYPE) || type.equals(Double.class)) {
				return (T) Double.valueOf(result.getDouble(1));
			
			} else if (type.equals(Float.TYPE) || type.equals(Float.class)) {
				return (T) Float.valueOf(result.getFloat(1));
			
			} else if (type.equals(Short.TYPE) || type.equals(Short.class)) {
				return (T) Short.valueOf(result.getShort(1));
				
			} else if(type.equals(Boolean.class) || type.equals(Boolean.TYPE)){
				return (T) Boolean.valueOf(result.getBoolean(1));
			
			} else if (type.equals(Byte.TYPE) || type.equals(Byte.class)) {
				return (T) Byte.valueOf(result.getByte(1));
			
			} else if(DataConverterProvider.getConverter() != null){
				return (T) DataConverterProvider.getConverter().ColumnToField(result.getString(1), type, result.getMetaData().getColumnTypeName(1), 1, result);
			} else {
				return (T) result.getObject(1);
			}
		}
		return t;
	}
}
