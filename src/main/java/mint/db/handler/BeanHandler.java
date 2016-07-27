package mint.db.handler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import mint.db.BeanConverter;
import mint.db.ResultSetHandler;

/** 
 * bean处理器。将查询结果集的第一行封装成指定类型的bean后返回。
 * 指定的bean需要符合bean规范，对应的属性要写上getter 和 setter
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:43:01 
 * 
 * @param <T> 
 */
public class BeanHandler<T> implements ResultSetHandler<T>{
	private Class<T> beanClass = null;
	private Map<String, String> columnFieldMap = null;
	
	/**
	 * @param beanClass
	 */
	public BeanHandler(Class<T> beanClass){
		this.beanClass = beanClass;
	}
	
	/**
	 * @param beanClass
	 * @param columnFieldMap 数据库列名和bean字段名的对应关系
	 */
	public BeanHandler(Class<T> beanClass, Map<String, String> columnFieldMap){
		this.beanClass = beanClass;
		this.columnFieldMap = columnFieldMap;
	}
	
	@Override
	public T handle(ResultSet result) throws SQLException {
		return BeanConverter.toBean(beanClass, result, columnFieldMap);
	}
}
