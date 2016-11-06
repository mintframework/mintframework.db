package mint.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import mint.db.MiniConnectionPool;
import mint.db.SQLExecutor;
import mint.db.Transaction;
import mint.db.bean.User;

public class AppTest {
	private static MiniConnectionPool connectionPool;

	static {
		MysqlConnectionPoolDataSource dataSource;
		dataSource = new MysqlConnectionPoolDataSource();
		dataSource.setUser("root");
		dataSource.setPassword("root");
		dataSource.setServerName("localhost");
		dataSource.setPort(3306);
		dataSource.setDatabaseName("jdbc");
		/*三个参数分别是：数据源、最大连接数和获取连接的超时时间（单位是秒）*/
		connectionPool = new MiniConnectionPool(dataSource, 20, 120);
	}
	
	/**
	 * 插入数据
	 * @throws SQLException
	 */
	public void insert() throws SQLException{
		Connection connection = connectionPool.getConnection();
		
		SQLExecutor runner = new SQLExecutor();
		String sql = "insert into user(id, username, password, update_time) values(?,?,?,?)";
		runner.update(connection, sql, 1, "水牛叔叔", "nooneknows", new Date().getTime());
		
		/*所有的connection都需要自己关闭*/
		connection.close();
	}
	
	/**
	 * 查询多条数据并封装成对象。
	 * 当类的字段名和数据库对应字段名不同时，需要一个map生命他们之间映射关系
	 * 数据库字段名->类字段名
	 * @throws SQLException
	 */
	public void queryBeanList() throws SQLException{
		SQLExecutor runner = new SQLExecutor();
		
		String sql = "select * from user limit 0, 1000";
		
		/* 类的字段名和数据库对应字段名不同时，需要一个map生命他们之间映射关系
		 * 数据库字段名->类字段名
		 */
		Map<String, String> map = new HashMap<String, String>();
		map.put("create_time", "createTime");
		
		Connection connection = connectionPool.getConnection();
		List<User> users = runner.selectBeanList(connection, User.class, null, sql);
		
		for(User u : users){
			System.out.println(u.getUsername());
		}
		
		connection.close();
	}
	
	/**
	 * 查询单条数据并封装成对象。
	 * 当类的字段名和数据库对应字段名不同时，需要一个map生命他们之间映射关系
	 * @throws SQLException
	 */
	public void queryBean() throws SQLException{
		SQLExecutor runner = new SQLExecutor();
		
		String sql = "select * from user limit 0, 100";
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("create_time", "createTime");
		
		Connection connection = connectionPool.getConnection();
		User user = runner.selectBean(connection, User.class, null, sql);
		
		System.out.println(user.getUsername());
		
		connection.close();
	}
	
	/**
	 * 查询单个字段
	 * @throws SQLException
	 */
	public void queryScalar() throws SQLException{
		SQLExecutor runner = new SQLExecutor();
		
		String sql = "select count(id) from user";
		Connection connection = connectionPool.getConnection();
		Integer count = runner.selectScalar(connection, Integer.class, sql);
		
		System.out.println(count);
		connection.close();
	}
	
	/**
	 * 事务
	 * @throws SQLException
	 */
	public void testTransaction() throws SQLException{
		Connection conn = connectionPool.getConnection();
		Transaction trans = Transaction.startTransaction(conn);
		
		/*addItem采用动态参数*/
		trans.addItem("insert into user (id, username, age) values (?, ?, ?)", 5, "郑海英", 25);
		trans.addItem("insert into user (id, username, age) values (?, ?, ?)", 6, "卓琼羽", 23);
		trans.addItem("insert into user (id, username, age) values (?, ?, ?)", 7, "邱露", 23);
		trans.addItem("insert into user (id, username, age) values (?, ?, ?)", 8, "张洁", 23);
		
		trans.commit();
		trans.close();
	}
}
