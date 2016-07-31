package mint.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import mint.db.SQLRunner;

/** 
 * 事务支持的封装
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:42:43 
 *  
 */
public class Transaction {
	final Connection conn;
	final List<TransactionItem> items;
	
	private Transaction(Connection conn){
		this.conn = conn;
		this.items = new ArrayList<TransactionItem>();
	}
	
	/**
	 * 根据sql 语句和动态预处理参数添加事务操作项
	 * @param sql
	 * @param params 动态参数
	 */
	public Transaction addItem(String sql, Object... params){
		this.items.add(new TransactionItem(sql, params));
		return this;
	}
	
	/**
	 * 根据sql 语句 和 数组预处理参数添加事务操作项
	 * @param sql
	 * @param params
	 * @return
	 */
	public Transaction addItem1(String sql, Object[] params){
		this.items.add(new TransactionItem(sql, params));
		return this;
	}
	
	/**
	 * 添加一个事务操作项
	 * @param item
	 */
	public Transaction addItem(TransactionItem item){
		this.items.add(item);
		return this;
	}
	
	/**
	 * 添加一批事务操作项
	 * @param items
	 */
	public Transaction addItems(List<TransactionItem> items){
		this.items.addAll(items);
		return this;
	}
	
	/**
	 * 开启事务
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	public static Transaction startTransaction(Connection conn) throws SQLException {
		Transaction ts;
		try {
			conn.setAutoCommit(false);
			ts = new Transaction(conn);
		} catch (SQLException e) {
			throw e;
		}
		
		return ts;
	}

	/**
	 * 手动提交事务
	 * @throws SQLException 
	 */
	public void commit() throws SQLException {
		try {
			addAllItems();
			conn.commit();
		} catch (SQLException e) {
			throw e;
		}
	}
	
	/**
	 *手动回滚事务
	 * @throws SQLException 
	 */
	public void rollback() throws SQLException {
		try {
			conn.rollback();
		} catch (SQLException e) {
			throw e;
		}
	}

	/**
	 * 手动关闭连接
	 * @throws SQLException 
	 */
	public void close() throws SQLException {
		try {
			conn.close();
		} catch (SQLException e) {
			throw e;
		}
	}
	
	/**
	 * @return 返回被事务所使用的connection
	 */
	public Connection getConnection(){
		return this.conn;
	}
	
	private SQLRunner addAllItems() throws SQLException{
		SQLRunner runner = new SQLRunner();
		for(TransactionItem item : items){
			if((item.params == null || item.params instanceof Object[]) && !(item.params instanceof Object[][])){
				runner.update(conn, item.sql, (Object[])item.params);
			} else if(item.params instanceof Object[][] && !(item.params instanceof Object[][][])) {
				runner.batch(conn, item.sql, (Object[][])item.params);
			}
		}
		
		return runner;
	}
	
	private boolean exe(){
		if(items==null || items.size()==0){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return true;
		}
		
		Boolean result;
		try {
			addAllItems();
			conn.commit();
			result = true;
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			result = false;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	/**
	 * 自动执行已添加的事务，事务失败是自动回滚。执行完毕后自动关闭连接
	 * @return 事务执行成功与否
	 */
	public boolean execute(){
		return exe();
	}
	
	/**
	 * 自动用给定的操作执行事务，事务失败时自动回滚。执行完毕后自动关闭连接
	 * @param items 事务操作项
	 * @return 事务执行成功与否
	 */
	public boolean execute(List<TransactionItem> items){
		addItems(items);
		return exe();
	}
}
