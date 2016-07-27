package demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import mint.db.MiniConnectionPool;
import mint.db.SQLRunner;
import mint.db.Transaction;
import mint.db.handler.BeanHandler;
import mint.db.handler.BeanListHandler;
import mint.db.handler.ScalarHandler;

public class TestDB {
	private static MiniConnectionPool connectionPool;

	static{
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
	@Test
	public void insert() throws SQLException{
		Connection connection = connectionPool.getConnection();
		
		SQLRunner runner = new SQLRunner(connection);
		String sql = "insert into user(id, username, password, update_time) values(?,?,?,?)";
		runner.update(sql, 1, "水牛叔叔", "nooneknows", new Date().getTime());
		
		/*所有的connection都需要自己关闭*/
		connection.close();
	}
	
	/**
	 * 查询多条数据并封装成对象。
	 * 当类的字段名和数据库对应字段名不同时，需要一个map生命他们之间映射关系
	 * 数据库字段名->类字段名
	 * @throws SQLException
	 */
	@Test
	public void queryList() throws SQLException{
		SQLRunner runner = new SQLRunner();
		
		String sql = "select * from user limit 0, 1000";
		
		/* 类的字段名和数据库对应字段名不同时，需要一个map生命他们之间映射关系
		 * 数据库字段名->类字段名
		 */
		Map<String, String> map = new HashMap<String, String>();
		map.put("create_time", "createTime");
		
		Connection connection = connectionPool.getConnection();
		List<User> users = runner.query(connection, sql, new BeanListHandler<User>(User.class, map));
		
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
	@Test
	public void queryBean() throws SQLException{
		SQLRunner runner = new SQLRunner();
		
		String sql = "select * from user limit 0, 100";
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("create_time", "createTime");
		
		Connection connection = connectionPool.getConnection();
		User user = runner.query(connection, sql, new BeanHandler<User>(User.class, map));
		
		System.out.println(user.getUsername());
		
		connection.close();
	}
	
	/**
	 * 查询单个字段
	 * @throws SQLException
	 */
	@Test
	public void queryScalar() throws SQLException{
		SQLRunner runner = new SQLRunner();
		
		String sql = "select count(id) from user";
		Connection connection = connectionPool.getConnection();
		String count = runner.query(connection, sql, new ScalarHandler<String>(String.class));
		
		System.out.println(count);
		connection.close();
	}
	
	/**
	 * 事务
	 * @throws SQLException
	 */
	@Test
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
