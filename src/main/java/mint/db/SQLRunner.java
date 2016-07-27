package mint.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** 
 * sql语句执行器。
 * SQLRunner用到的所有 Connection都要使用者自己关闭，否则资源无法释放。<br/>
 * 本类中，除了构造方法外，所有不带connection的方法都是非线程安全的；所有带有connection的方法都是线程安全的
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:42:05 
 *  
 */
public final class SQLRunner {
	private Connection conn;
	private static DataConverter<?> converter;
	
	public SQLRunner(){
		
	}
	
	/**
	 * 带有connection的构造函数。本对象所有不指定connection的方法都使用在构造函数指定的connection
	 * @param conn
	 * @throws SQLException 
	 */
	public SQLRunner(Connection conn) {
		this.conn = conn;
	}
	
	/**
	 * 非线程安全。<br/>
	 * 批量执行sql语句。一般用来执行insert、update、create等语句，而不执行select 语句
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public int[] batch(String sql, Object[][] params) throws SQLException{
		return batch(conn, sql, params);
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
	 * 非线程安全。<br/>
	 * 执行不带参数的更新语句，包括insert、update、delete、create等语句
	 * @param sql
	 * @throws SQLException 
	 */
	public int update(String sql) throws SQLException {
		return update(conn, sql);
	}
	
	/**
	 * 非线程安全。<br/>
	 * 执行带参数的更新语句，包括insert、update、delete、create等语句
	 * @param sql
	 * @param params parameters for preparestatement
	 * @throws SQLException
	 */
	public int update(String sql, Object... params) throws SQLException{
		return update(conn, sql, params);
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
	 * 非线程安全。<br/>
	 * 执行无参查询语句。like ：select ?, ? from user where id = ?。
	 * @param sql
	 * @return 
	 * @throws SQLException 
	 */
	public <T> T query(String sql, ResultSetHandler<T> handler) throws SQLException {
		return query(conn, sql, handler);
	}
	
	/**
	 * 非线程安全。<br/>
	 * 执行无参查询语句。like ：select id, name from user where id = ?。
	 * @param sql
	 * @return 
	 * @throws SQLException 
	 */
	public <T> T query(String sql, ResultSetHandler<T> handler, Object... params) throws SQLException {
		return query(conn, sql, handler, params);
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
	 * 返回构造函数给定的connection
	 * @return
	 */
	public Connection getConnection(){
		return conn;
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
}
