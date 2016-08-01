// Copyright 2007-2011 Christian d'Heureuse, Inventec Informatik AG, Zurich, Switzerland
// www.source-code.biz, www.inventec.ch/chdh
//
// This module is multi-licensed and may be used under the terms
// of any of the following licenses:
//
//  EPL, Eclipse Public License, http://www.eclipse.org/legal
//  LGPL, GNU Lesser General Public License, http://www.gnu.org/licenses/lgpl.html
//  MPL, Mozilla Public License 1.1, http://www.mozilla.org/MPL
//
// Please contact the author if you need another license.
// This module is provided "as is", without warranties of any kind.

package mint.db;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;//池化connection对象的工厂
import javax.sql.PooledConnection;

/**
* A lightweight standalone JDBC connection pool manager.
*
* <p>The public methods of this class are thread-safe.
*
* <p>Home page: <a href="http://www.source-code.biz/miniconnectionpoolmanager">www.source-code.biz/miniconnectionpoolmanager</a><br>
* Author: Christian d'Heureuse, Inventec Informatik AG, Zurich, Switzerland<br>
* Multi-licensed: EPL / LGPL / MPL.
*/
public class MiniConnectionPool {
	
	private ConnectionPoolDataSource		dataSource;						//池化connection对象的工厂
	private int								maxConnections;
	private long							timeoutMs;
	private PrintWriter						logWriter;						//输出log
	private Semaphore						semaphore;						//信号量
	private PoolConnectionEventListener		poolConnectionEventListener;
	
	// The following variables must only be accessed within synchronized blocks.
	// @GuardedBy("this") could by used in the future.
	private LinkedList<PooledConnection>	recycledConnections;			// list of inactive PooledConnections
	private int								activeConnections;				// number of active (open) connections of this pool
	private boolean							isDisposed;						// true if this connection pool has been disposed
	private boolean							doPurgeConnection;				// flag to purge the connection currently beeing closed instead of recycling it
	private PooledConnection				connectionInTransition;			// a PooledConnection which is currently within a PooledConnection.getConnection() call, or null
	private boolean							debugModel = false;	
	/**
	* Thrown in {@link #getConnection()} or {@link #getValidConnection()} when no free connection becomes
	* available within <code>timeout</code> seconds.
	*/
	public class TimeoutException extends RuntimeException {
		private static final long serialVersionUID = 1;

		public TimeoutException() {
			super("Timeout while waiting for a free database connection(poolSize:"+maxConnections+"activeCount:"+activeConnections+")");
		}

		public TimeoutException(String msg) {
			super(msg);
		}
	}

	/**
	 * debugModel为true时，每次获取connection会报告连接池的连接数据
	 * @param debugModel
	 */
	public void setDebugModel(boolean debugModel){
		this.debugModel = debugModel;
	}
	
	/**
	* Constructs a MiniConnectionPoolManager object.
	*
	* @param dataSource
	*    the data source for the connections.
	* @param maxConnections
	*    the maximum number of connections.
	* @param timeout
	*    the maximum time in seconds to wait for a free connection.
	*/
	public MiniConnectionPool(ConnectionPoolDataSource dataSource, int maxConnections, int timeout) {
		this.dataSource = dataSource;
		this.maxConnections = maxConnections;
		this.timeoutMs = timeout * 1000L;
		
		try {
			logWriter = dataSource.getLogWriter();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (maxConnections < 1) {
			throw new IllegalArgumentException("Invalid maxConnections value.");
		}
		semaphore = new Semaphore(maxConnections, true);
		recycledConnections = new LinkedList<PooledConnection>();// the pool?
		poolConnectionEventListener = new PoolConnectionEventListener();
	}
	
	/**
	* Closes all unused pooled connections.
	* 销毁整个连接池。要关闭所有连接
	*/
	public synchronized void dispose() throws SQLException {
		if (isDisposed) {
			return;
		}
		isDisposed = true;
		SQLException e = null;
		PooledConnection pconn;
		while (!recycledConnections.isEmpty()) {
			pconn = recycledConnections.remove();
			try {
				pconn.close();
			} catch (SQLException e2) {
				if (e == null) {
					e = e2;
				}
			}
		}
		
		if (e != null) {
			throw e;
		}
	}

	/**
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		// This routine is unsynchronized, because semaphore.tryAcquire() may
		// block.
		
		synchronized (this) {
			if (isDisposed) {
				throw new IllegalStateException("Connection pool has been disposed.");
			}
		}
		try {
			if (!semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while waiting for a database connection. pool size:"+maxConnections+"activeCount:"+activeConnections, e);
		}
		
		boolean ok = false;
		try {
			Connection conn = getConnection2();
			ok = true;
			
			if(debugModel){
				log("report after getConnection-> activeConnections:"+activeConnections+
						";freeConnections:"+recycledConnections.size()+
						";maxConnections:"+maxConnections);
			}
			
			return conn;
		} finally {
			if (!ok) {
				semaphore.release();
			}
		}
	}

	/**
	 * @return
	 */
	public synchronized int getActiveConnections() {
		return activeConnections;
	}

	/**
	 * @return
	 */
	public synchronized int getInactiveConnections() {
		return recycledConnections.size();
	}

	private synchronized Connection getConnection2() throws SQLException {
		if (isDisposed) { // test again within synchronized lock
			throw new IllegalStateException("Connection pool has been disposed.");
		}
		PooledConnection pconn;
		
		if (!recycledConnections.isEmpty()) {
			pconn = recycledConnections.remove();
		} else {
			pconn = dataSource.getPooledConnection();
			pconn.addConnectionEventListener(poolConnectionEventListener);
		}
		Connection conn;
		try {
			// The JDBC driver may call ConnectionEventListener.connectionErrorOccurred()
			// from within PooledConnection.getConnection(). To detect this within
			// disposeConnection(), we temporarily set connectionInTransition.
			
			/*防止游离的pooledConnection产生*/
			connectionInTransition = pconn;
			conn = pconn.getConnection();
		} finally {
			connectionInTransition = null;
		}
		activeConnections++;
		assertInnerState();
		
		return conn;
	}

	private synchronized void recycleConnection(PooledConnection pconn) {
		if (isDisposed || doPurgeConnection) {
			disposeConnection(pconn);
			return;
		}
		if (activeConnections <= 0) {
			throw new AssertionError();
		}
		activeConnections --;
		semaphore.release();
		recycledConnections.add(pconn);
		
		if(debugModel){
			log("report after recycleConnection-> activeConnections:"+activeConnections+
					";freeConnections:"+recycledConnections.size()+
					";maxConnections:"+maxConnections);
		}
		
		assertInnerState();//用于抛出异常（所有的connection数>maxConnections）
	}

	private synchronized void disposeConnection(PooledConnection pconn) {
		pconn.removeConnectionEventListener(poolConnectionEventListener);
		if (!recycledConnections.remove(pconn) && pconn != connectionInTransition) {
			if (activeConnections <= 0) {
				throw new AssertionError();
			}
			activeConnections--;
			semaphore.release();
		}
		try {
			pconn.close();
		} catch (SQLException e) {
			log("Error while closing database connection: " + e.toString());
		}
		assertInnerState();//用于抛出异常（所有的connection数>maxConnections）
	}

	private void log(String msg) {
		String s = "MiniConnectionPoolManager: " + msg;
		try {
			if (logWriter == null) {
				System.err.println(s);
			} else {
				logWriter.println(s);
			}
		} catch (Exception e) {
		}
	}

	private synchronized void assertInnerState() {
		if (activeConnections < 0 || 
				activeConnections + recycledConnections.size() > maxConnections ||
				activeConnections + semaphore.availablePermits() > maxConnections) {
			
			throw new AssertionError();
		}
	}

	private class PoolConnectionEventListener implements ConnectionEventListener {
		public void connectionClosed(ConnectionEvent event) {
			recycleConnection((PooledConnection) event.getSource());
		}

		public void connectionErrorOccurred(ConnectionEvent event) {
			disposeConnection((PooledConnection) event.getSource());
		}
	}
}