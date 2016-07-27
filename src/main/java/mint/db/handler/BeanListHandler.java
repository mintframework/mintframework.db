package mint.db.handler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import mint.db.BeanConverter;
import mint.db.ResultSetHandler;

/** 
 * bean 列表处理器。将查询结果集的第一行封装成指定类型的bean 列表后返回。
 * 指定的bean需要符合bean规范，对应的属性要写上getter 和 setter
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:43:20 
 * 
 * @param <T> 
 */
public class BeanListHandler<T> implements ResultSetHandler<List<T>>{
	private Class<T> beanClass = null;
	private Map<String, String> columnFieldMap = null;
	
	public BeanListHandler(Class<T> beanClass){
		this.beanClass = beanClass;
	}
	
	/**
	 * @param beanClass
	 * @param columnFieldMap 数据库列名和bean字段名的对应关系
	 */
	public BeanListHandler(Class<T> beanClass, Map<String, String> columnFieldMap){
		this.beanClass = beanClass;
		this.columnFieldMap = columnFieldMap;
	}
	
	@Override
	public List<T> handle(ResultSet result) throws SQLException {
		return BeanConverter.toBeanList(beanClass, result, columnFieldMap);
	}
}
