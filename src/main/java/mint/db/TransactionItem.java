package mint.db;

/** 
 *  封装事务处理操作，包括sql语句和执行参数。
 * 配合<code>TransactionService</code>使用
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:42:27 
 *  
 */
public final class TransactionItem {
	protected String sql;
	protected Object params;
	
	
	public TransactionItem(){
		
	}
	
	/**
	 * @param sql 事务的执行语句
	 */
	public TransactionItem(String sql){
		this.sql = sql;
	}
	
	/**
	 * @param sql 事务的执行语句
	 * @param params 事务的执行参数，二维数组或一维数组
	 */
	public TransactionItem(String sql, Object params){
		this.sql = sql;
		this.params = params;
	}
	
	/**
	 * 修改事务的执行语句
	 * @param sql 事务的执行语句
	 * @return 对象本身
	 */
	public TransactionItem setSql(String sql){
		this.sql = sql;
		return this;
	}
	
	/**
	 * 修改事务的参数
	 * @param params 事务的参数
	 * @return 对象本身
	 */
	public TransactionItem setParams(Object... params){
		this.params = params;
		return this;
	}
}
