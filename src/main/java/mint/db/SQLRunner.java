package mint.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * sql语句执行器。
 * SQLRunner用到的所有 Connection都要使用者自己关闭，否则资源无法释放。<br/>
 * 本类中，除了构造方法外，所有不带connection的方法都是非线程安全的；所有带有connection的方法都是线程安全的
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:42:05 
 *  
 */
/**
 * @author lwei
 *
 */
public final class SQLRunner {
	private static DataConverter<?> converter;
	
	public SQLRunner(){
		
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
		if(params == null || params.length == 0) return update(conn, sql);
		
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
	 * 线程安全。<br/>
	 * 查询结果并封装成对象
	 * @param conn
	 * @param sql
	 * @param handler 结果处理器
	 * @return
	 * @throws SQLException
	 */
	public <T> T query(Connection conn, String sql, ResultSetHandler<T> handler) throws SQLException{
		Statement stm = null;
		try{
			stm = conn.createStatement();
			ResultSet result = stm.executeQuery(sql);
			
			return handler.handle(result);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
	}
	
	/**
	 * 线程安全。<br/>
	 * 查询结果并封装成对象
	 * @param conn
	 * @param beanClass
	 * @param handler 结果处理器。把查询结果封装成对象
	 * @param params
	 * @throws SQLException 
	 */
	public <T> T query(Connection conn, String sql, ResultSetHandler<T> handler, Object... params) throws SQLException{
		if(params == null || params.length == 0) return query(conn, sql, handler);
		
		PreparedStatement pstm = null;
		try {
			pstm = conn.prepareStatement(sql);
			fillStatement(pstm, params, conn);
			return handler.handle(pstm.executeQuery());
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
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
				pstm.setString(i+1, (String) value);
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
				
			} else if(converter != null){
				pstm.setObject(i+1, converter.fieldToColumn(value, connection));
				
			} else {
				pstm.setObject(i+1, value);
			}
		}
	}
	
	private void closeStm(Statement stm) throws SQLException{
		if(stm == null) return;
		stm.close();
	}

	protected static void setConverter(DataConverter<?> converter) {
		SQLRunner.converter = converter;
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
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
		return BeanConverter.toBean(beanClass, result, columnFieldMap);
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
		if(params == null || params.length == 0) return selectBean(connection, beanClass, columnFieldMap, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		try {
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
		return BeanConverter.toBean(beanClass, result, columnFieldMap);
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
		} catch (SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
		return BeanConverter.toBeanList(beanClass, result, columnFieldMap);
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
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
		return BeanConverter.toBeanList(beanClass, result, columnFieldMap);
	}
	
	/**
	 * 
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public ResultMap selectMap(Connection connection, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) return selectMap(connection, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery();
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
		return createMap(result);
	}
	
	/**
	 * @param connection
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultMap selectMap(Connection connection, String sql) throws SQLException{
		Statement stm = null;
		ResultSet result = null;
		try{
			stm = connection.createStatement();
			result = stm.executeQuery(sql);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
		return createMap(result);
	}
	
	
	/**
	 * @param connection
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public List<ResultMap> selectResultMapList(Connection connection, String sql, Object... params) throws SQLException{
		if(params == null || params.length == 0) return selectResultMapList(connection, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery(sql);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
		return createMapList(result);
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
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
		return createMapList(result);
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
		
		if(params == null || params.length == 0) return selectResultMapMap(connection, keyColumn, sql);
		
		PreparedStatement pstm = null;
		ResultSet result = null;
		
		try{
			pstm = connection.prepareStatement(sql);
			fillStatement(pstm, params, connection);
			result = pstm.executeQuery(sql);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
		return createMapMap(keyColumn, result);
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
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
		return createMapMap(keyColumn, result);
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
			result = pstm.executeQuery(sql);
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(pstm);
		}
		
		return createScalar(clazz, result);
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
		} catch(SQLException e) {
			throw e;
		} finally {
			closeStm(stm);
		}
		
		return createScalar(clazz, result);
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
			
			for(int i=1,j=meta.getColumnCount(); i<=j; i++){
				map.put(meta.getColumnName(i), result.getString(i));
			}
			return map;
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
			do {
				ResultMap map = new ResultMap();
				for(int i=1,j=meta.getColumnCount(); i<=j; i++){
					map.put(meta.getColumnName(i), result.getString(i));
				}
				mapList.add(map);
				
			} while(result.next());
			
			return mapList;
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
			
			} else if(DataConverterProvider.getConverter() != null){
				return (T) DataConverterProvider.getConverter().ColumnToField(result.getString(1), type, result.getMetaData().getColumnTypeName(1), 1, result);
			} else {
				return (T) result.getObject(1);
			}
		}
		return t;
	}
}
