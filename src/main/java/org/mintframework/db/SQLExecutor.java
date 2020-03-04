package org.mintframework.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * sql语句执行器。
 * SQLRunner用到的所有 Connection都要使用者自己关闭，否则资源无法释放。<br/>
 * 本类中，除了构造方法外，所有不带connection的方法都是非线程安全的；所有带有connection的方法都是线程安全的
 * @author LiangWei(cnliangwei@foxmail.com)
 * @date 2015年3月13日 下午9:42:05 
 *  
 */
/**
 * @author lwei
 *
 */
public final class SQLExecutor  {
	private FieldColumnConverter<?> converter;
	
	public FieldColumnConverter<?> getConverter() {
		return converter;
	}

	public void setConverter(FieldColumnConverter<?> converter) {
		this.converter = converter;
	}

	private Boolean autoUnderlineToCamelhump = false;
	
	/**
	 * 是否将查询出来的字段名自动有下划线命名，转成驼峰命名
	 * @return
	 */
	public Boolean getAutoUnderlineToCamelhump() {
		return autoUnderlineToCamelhump;
	}

	/**
	 * 是否将查询出来的字段名自动有下划线命名，转成驼峰命名
	 * @param autoUnderlineToCamelhump
	 */
	public void setAutoUnderlineToCamelhump(Boolean autoUnderlineToCamelhump) {
		this.autoUnderlineToCamelhump = autoUnderlineToCamelhump;
	}
	
	public SQLExecutor(){
		
	}
	
	public SQLExecutor(FieldColumnConverter<?> converter){
		this.converter = converter;
	}
	
	/**
	 * 线程安全。<br/>
	 * 批量执行sql语句。一般用来执行insert、update、create等语句，而不执行select 语句
	 * @param conn
	 * @param sql
	 * @param params 必须是二维数组
	 * @return
	 * @throws SQLException
	 */
	public int[] batch(Connection conn, String sql, Object[][] params) throws SQLException{
		if(params == null || params.length == 0 || !(params instanceof Object[][]) || (params instanceof Object[][][]) ){
			throw new SQLException("invalidate params:"+params);
		}
		
		PreparedStatement pstm = null;
		try {
			pstm = conn.prepareStatement(sql);
			
			for(Object[] ps : params){
				fillStatement(pstm, ps, conn);
				pstm.addBatch();
			}
			
			return pstm.executeBatch();
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
	}
	
	/**
	 * 线程安全。<br/>
	 * 批量执行sql语句。一般用来执行insert、update、create等语句，而不执行select 语句
	 * @param conn
	 * @param sql
	 * @param params 必须是二维数组
	 * @return
	 * @throws SQLException
	 */
	public void batch(Connection conn, String[] sqls, Object[][] params) throws SQLException{
		if(params == null || params.length == 0 || sqls == null || sqls.length == 0 || !(params instanceof Object[][]) || (params instanceof Object[][][]) ){
			throw new SQLException("invalidate params:"+params);
		}
		
		if(sqls.length != params.length) {
			throw new RuntimeException("sqls count donot match params count");
		}
		
		PreparedStatement pstm = null;
		try {
			int index = 0;
			for(String sql : sqls) {
				pstm = conn.prepareStatement(sql);
				fillStatement(pstm, params[index], conn);
				pstm.execute();
				index++;
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
	}
	
	/**
	 * 线程安全。<br/>
	 * 执行不带参数的更新语句，包括insert、update、delete、create等语句
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public int update(Connection conn, String sql) throws SQLException {
		Statement stm = null;
		try {
			stm = conn.createStatement();
			return stm.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
	}
	
	/**
	 * 线程安全。<br/>
	 * 执行带参数的更新语句，包括insert、update、delete、create等语句
	 * @param conn
	 * @param sql 查询语句
	 * @param params 查询参数
	 * @throws SQLException
	 */
	public int update(Connection conn, String sql, Object... params) throws SQLException {
		if(params == null || params.length == 0) {
			return update(conn, sql);
		}
		
		PreparedStatement pstm = null;
		try {
			pstm = conn.prepareStatement(sql);
			fillStatement(pstm, params, conn);
			return pstm.executeUpdate();
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
	}
	
	/**
	 * @param connection
	 * @param beanClass
	 * @param columnFieldMap
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public <T> T selectBean(Connection connection, Class<T> beanClass, Map<String, String> columnFieldMap, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return BeanConverter.toBean(beanClass, result, columnFieldMap, this.converter);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
	}
	
	/**
	 * @param connection
	 * @param beanClass
	 * @param columnFieldMap
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public <T> T selectBean(Connection connection, Class<T> beanClass, Map<String, String> columnFieldMap, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) {
			return selectBean(connection, beanClass, columnFieldMap, sql);
		}
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		try {
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return BeanConverter.toBean(beanClass, result, columnFieldMap, converter);
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
	}
	
	/**
	 * 
	 * @param beanClass
	 * @param columnFieldMap
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public <T> List<T> selectBeanList(Connection connection, Class<T> beanClass, Map<String, String> columnFieldMap, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) return selectBeanList(connection, beanClass, columnFieldMap, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		try {
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return BeanConverter.toBeanList(beanClass, result, columnFieldMap, converter);
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
	}
	
	/**
	 * @param connection
	 * @param beanClass
	 * @param columnFieldMap
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public <T> List<T> selectBeanList(Connection connection, Class<T> beanClass, Map<String, String> columnFieldMap, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return BeanConverter.toBeanList(beanClass, result, columnFieldMap, converter);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
	}
	
	/**
	 * 
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public ResultMap selectResultMap(Connection connection, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) {
			return selectResultMap(connection, sql);
		}
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return createMap(result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
	}
	
	/**
	 * @param connection
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultMap selectResultMap(Connection connection, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return createMap(result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
	}
	
	
	/**
	 * @param connection
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<ResultMap> selectResultMapList(Connection connection, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) {
			return selectResultMapList(connection, sql);
		}
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return createMapList(result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
	}
	
	/**
	 * @param connection
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public List<ResultMap> selectResultMapList(Connection connection, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return createMapList(result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
	}
	
	/**
	 * @param keyColumn
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public Map<String, ResultMap> selectResultMapMap(Connection connection, String keyColumn, String sql, Object... params) throws SQLException{
		if(keyColumn==null || "".equals(keyColumn)){
			throw new RuntimeException("keyColumn　can not be empty");
		}
		
		if(params == null || params.length == 0) {
			return selectResultMapMap(connection, keyColumn, sql);
		}
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return createMapMap(keyColumn, result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
	}
	
	/**
	 * @param connection
	 * @param keyColumn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public Map<String, ResultMap> selectResultMapMap(Connection connection, String keyColumn, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return createMapMap(keyColumn, result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
	}
	
	/**
	 * @param connection
	 * @param clazz
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public <T> T  selectScalar(Connection connection, Class<T> clazz, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) return selectScalar(connection, clazz, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return createScalar(clazz, result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
	}
	
	/**
	 * @param connection
	 * @param clazz
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public <T> T  selectScalar(Connection connection, Class<T> clazz, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return createScalar(clazz, result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
	}
	
	/**
	 * @param connection
	 * @param clazz
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public <T> List<T> selectScalarList(Connection connection, Class<T> clazz, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) return selectScalarList(connection, clazz, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
			return createScalarList(clazz, result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
	}
	
	/**
	 * @param connection
	 * @param clazz
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public <T> List<T> selectScalarList(Connection connection, Class<T> clazz, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
			return createScalarList(clazz, result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
	}
	
	/**
	 * 根据resultset生成ResultMap
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	private ResultMap createMap(ResultSet result) throws SQLException{
		if(result.next()){
			ResultSetMetaData meta = result.getMetaData();
			ResultMap map = new ResultMap();
			
			if(autoUnderlineToCamelhump){
				List<String> columnMap = autoUnderlineToCamelhump(result.getMetaData());
				for(int i=1,j=meta.getColumnCount()+1; i<j; i++){
					map.put(columnMap.get(i-1), result.getString(i));
				}
				return map;
			} else {
				for(int i=1,j=meta.getColumnCount()+1; i<j; i++){
					map.put(meta.getColumnLabel(i), result.getString(i));
				}
				return map;
			}
		}
		
		return null;
	}
	
	/**
	 * 根据resultset生成List<ResultMap>
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	private List<ResultMap> createMapList(ResultSet result) throws SQLException{
		if(result.next()){
			ResultSetMetaData meta = result.getMetaData();
			
			List<ResultMap> mapList = new ArrayList<ResultMap>();
			if(autoUnderlineToCamelhump){
				List<String> columnMap = autoUnderlineToCamelhump(result.getMetaData());
				do {
					ResultMap map = new ResultMap();
					for(int i=1,j=meta.getColumnCount(); i<=j; i++){
						map.put(columnMap.get(i-1), result.getString(i));
					}
					mapList.add(map);
				} while(result.next());
				
				return mapList;
			} else {
				do {
					ResultMap map = new ResultMap();
					for(int i=1,j=meta.getColumnCount(); i<=j; i++){
						map.put(meta.getColumnName(i), result.getString(i));
					}
					mapList.add(map);
				} while(result.next());
				
				return mapList;
			}
		}
		
		return null;
	}
	
	/**
	 * @param keyColumn
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	private Map<String, ResultMap> createMapMap(String keyColumn, ResultSet result) throws SQLException{
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
			
			Map<String, ResultMap> mapMap = new HashMap<String, ResultMap>();
			String key;
			
			//是否将查询出来的字段名自动有下划线命名，转成驼峰命名
			if(autoUnderlineToCamelhump){
		        keyColumn = getCamelhumpString(keyColumn);
		        List<String> columnMap = autoUnderlineToCamelhump(result.getMetaData());
				do {
					ResultMap map = new ResultMap();
					for(int i=1,j=meta.getColumnCount(); i<=j; i++){
						key = columnMap.get(i-1);
						map.put(key, result.getString(i));
						if(key.equals(keyColumn)){
							mapMap.put(result.getString(i), map);
						}
					}
				} while(result.next());
			} else {
				do {
					ResultMap map = new ResultMap();
					for(int i=1,j=meta.getColumnCount(); i<=j; i++){
						key = meta.getColumnName(i);
						map.put(key, result.getString(i));
						if(key.equals(keyColumn)){
							mapMap.put(result.getString(i), map);
						}
					}
					
				} while(result.next());
			}
			
			return mapMap;
		}
		
		return null;
	}
	
	/**
	 * @param type
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private <T> T createScalar(Class<T> type, ResultSet result) throws SQLException {
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
				
			} else if(type.isEnum()){
				return (T) BeanConverter.initEnum(result.getString(1), type);
				
			} else if(this.converter != null){
				return (T) this.converter.ColumnToField(result.getString(1), type, result.getMetaData().getColumnTypeName(1));
			} else {
				return (T) result.getObject(1);
			}
		}
		return t;
	}
	
	/**
	 * @param type
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private <T> List<T> createScalarList(Class<T> type, ResultSet result) throws SQLException {
		
		if(result.next()){		
			List<T> ts = new LinkedList<T>();
			
			if (type.equals(String.class)) {
				do {
					ts.add((T) result.getString(1));
				} while(result.next());
			} else if (type.equals(Integer.TYPE) || type.equals(Integer.class)) {
				do {
					ts.add((T) Integer.valueOf(result.getInt(1)));
				} while(result.next());
			} else if (type.equals(Boolean.TYPE) || type.equals(Boolean.class)) {
				do {
					ts.add((T) Boolean.valueOf(result.getBoolean(1)));
				} while(result.next());
			} else if (type.equals(Long.TYPE) || type.equals(Long.class)) {
				do {
					ts.add((T) Long.valueOf(result.getLong(1)));
				} while(result.next());
			} else if (type.equals(Double.TYPE) || type.equals(Double.class)) {
				do {
					ts.add((T) Double.valueOf(result.getDouble(1)));
				} while(result.next());
			} else if (type.equals(Float.TYPE) || type.equals(Float.class)) {
				do {
					ts.add((T) Float.valueOf(result.getFloat(1)));
				} while(result.next());
			} else if (type.equals(Short.TYPE) || type.equals(Short.class)) {
				do {
					ts.add((T) Short.valueOf(result.getShort(1)));
				} while(result.next());
			} else if(type.equals(Boolean.class) || type.equals(Boolean.TYPE)){
				do {
					ts.add((T) Boolean.valueOf(result.getBoolean(1)));
				} while(result.next());
			} else if (type.equals(Byte.TYPE) || type.equals(Byte.class)) {
				do {
					ts.add((T) Byte.valueOf(result.getByte(1)));
				} while(result.next());
			
			} else if(this.converter != null){
				do {
					ts.add((T) this.converter.ColumnToField(result.getString(1), type, result.getMetaData().getColumnTypeName(1)));
				} while(result.next());
			} else {
				do {
					ts.add((T) result.getObject(1));
				} while(result.next());
			}
			return ts;
		}
		
		return null;
	}

	/**
	 * 设置prepareStatement的参数
	 * @param pstm
	 * @param params 查询参数
	 * @throws SQLException
	 */
	private void fillStatement(PreparedStatement pstm, Object[] params, Connection connection) throws SQLException {
		if(params == null || params.length == 0) return;
		
		Object value;
		for(int i=0, len=params.length; i<len; i++){
			value = params[i];
			
			if(value instanceof String){
				pstm.setString(i+1, value.toString());
			} else if(value instanceof Integer) {
				pstm.setInt(i+1, (int) value);
				
			} else if(value instanceof Long) {
				pstm.setLong(i+1, (long) value);
				
			} else if(value instanceof Double) {
				pstm.setDouble(i+1, (double) value);
				
			} else if(value instanceof Boolean){
				pstm.setBoolean(i+1, (Boolean) value);
				
			} else if(value instanceof Float){
				pstm.setFloat(i+1, (float) value);
				
			} else if(value instanceof Short){
				pstm.setShort(i+1, (short) value);
				
			} else if(value instanceof Byte){
				pstm.setShort(i+1, (short) value);
				
			} else if(value instanceof Enum) {
				pstm.setObject(i+1, ((Enum<?>) value).ordinal());
				
			} else if(converter != null){
				pstm.setObject(i+1, converter.fieldToColumn(value));
				
			} else {
				pstm.setObject(i+1, value);
			}
		}
	}
	
	private void closeStm(Statement stm) throws SQLException{
		if(stm == null) return;
		stm.close();
	}

	//将查询出来的字段名自动有下划线命名，转成驼峰命名
	private List<String> autoUnderlineToCamelhump(ResultSetMetaData meta) throws SQLException{
		List<String> columnMap = new ArrayList<String>();
		Pattern pattern = Pattern.compile("_[a-z]");
		Matcher matcher;
		
		StringBuilder builder = new StringBuilder(30);
		for(int i=1,j=meta.getColumnCount()+1; i<j; i++){
			builder.setLength(0);
			builder.append(meta.getColumnLabel(i));
			
			matcher = pattern.matcher(meta.getColumnLabel(i));
			
			for (int x = 0; matcher.find(); x++) {
				builder.replace(matcher.start() - x, matcher.end() - x, matcher.group().substring(1).toUpperCase());
			}
			
			if (Character.isUpperCase(builder.charAt(0))) {
				builder.replace(0, 1, String.valueOf(Character.toLowerCase(builder.charAt(0))));
			}
			
			columnMap.add(builder.toString());
		}
		return columnMap;
	}
	
	/**
	 * @param str
	 * @return
	 */
	private String getCamelhumpString (String str){
		 Matcher matcher = Pattern.compile("_[a-z]").matcher(str);
	        StringBuilder builder = new StringBuilder(str);
	        for (int i = 0; matcher.find(); i++) {
	            builder.replace(matcher.start() - i, matcher.end() - i, matcher.group().substring(1).toUpperCase());
	        }
	        if (Character.isUpperCase(builder.charAt(0))) {
	            builder.replace(0, 1, String.valueOf(Character.toLowerCase(builder.charAt(0))));
	        }
	        return builder.toString();
	}
}