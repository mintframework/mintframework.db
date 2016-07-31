package mint.db;

import java.sql.ResultSet;
import java.sql.SQLException;

/** 
 * 
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:41:51 
 * 
 * @param <T> 
 */
public interface ResultSetHandler<T> {
	/**
	 * 
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	T handle(ResultSet result) throws SQLException;
}
