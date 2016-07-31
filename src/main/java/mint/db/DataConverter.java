package mint.db;

import java.sql.Connection;
import java.sql.ResultSet;

public interface DataConverter<T> {
	/**
	 * 将bean的field值序列化成数据库column的值
	 * @param fieldValue
	 * @return
	 */
	public Object fieldToColumn(Object fieldValue, Connection connection);
	
	/**
	 * 决定如何将数据库的数据转换成bean的field
	 * @param databaseValue
	 * @param fieldType
	 * @param columnType
	 * @param index
	 * @param result
	 * @return
	 */
	public Object ColumnToField(String databaseValue, Class<?> fieldType, String columnType, int columnIndex, ResultSet result);
}