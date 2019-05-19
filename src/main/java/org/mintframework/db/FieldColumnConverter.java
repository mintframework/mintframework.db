package org.mintframework.db;

public interface FieldColumnConverter<T> {
	/**
	 * 将bean的field值序列化成数据库column的值
	 * @param fieldValue
	 * @return
	 */
	public Object fieldToColumn(Object fieldValue);
	
	/**
	 * 决定如何将数据库的数据转换成bean的field
	 * @param databaseValue
	 * @param fieldType
	 * @param columnType
	 * @param index
	 * @param result
	 * @return
	 */
	public T ColumnToField(String columnValue, Class<?> fieldType, String columnType);
}