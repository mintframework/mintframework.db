package mint.db;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * 结果集到bean的转换器
 * @author LiangWei(895925636@qq.com)
 * @date 2015年3月13日 下午9:41:31 
 *  
 */
public class BeanConverter {
	private static DataConverter<?> dataConverter = null;
	
	private static final Map<Class<?>, Map<String, SetterInfo>> setterInfoMapMap = new HashMap<Class<?>, Map<String, SetterInfo>>(); 
	/**
	 * 将结果集的第一列转化成bean
	 * @param beanClass 
	 * @param result
	 * @param columnFieldMap 数据库列名和bean字段名的对应关系
	 * @return
	 * @throws SQLException 
	 */
	public static <T> T toBean(Class<T> beanClass, ResultSet result, Map<String, String> columnFieldMap) throws SQLException {
		T t = null;
		
		if(result.next()){
			t = createBean(beanClass, result, getEffectiveColumn(beanClass, result.getMetaData(), columnFieldMap));
		}
		
		return t;
	}
	
	
	/**
	 * 将结果集转换成bean列表
	 * @param beanClass
	 * @param result
	 * @param columnFieldMap 数据库列名和bean字段名的对应关系
	 * @return
	 * @throws SQLException 
	 */
	public static <T> List<T> toBeanList(Class<T> beanClass, ResultSet result, Map<String, String> columnFieldMap) throws SQLException{
		List<T> beanList = new ArrayList<T>();
		
		if(result.next()){
			SetterInfo[] infos = getEffectiveColumn(beanClass, result.getMetaData(), columnFieldMap);
			do{
				beanList.add(createBean(beanClass, result, infos));
			} while(result.next());
		}
		
		return beanList;
	}
	
	/**
	 * 从resultset创建一个bean
	 * @param claz
	 * @param rslt
	 * @param infos
	 * @return
	 * @throws SQLException
	 */
	private static <T> T createBean(Class<T> claz,ResultSet rslt, SetterInfo[] infos) throws SQLException{
		T t = null;
		try {
			t = claz.newInstance();
			SetterInfo info;
			for(int i=0,l=infos.length; i<l; i++){
				info = infos[i];
				if(info != null){
					info.method.invoke(t, processColumn(rslt, i+1, info.type));
				}
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
		
		return t;
		
	}
	
	/**
	 * map a SetterInfo to an effective column(column can set into given bean)
	 * and map null to uneffective columns
	 * 
	 * @param beanClass
	 * @param metaData
	 * @param columnFieldMap
	 * @return 
	 * @throws SQLException 
	 */
	private static SetterInfo[] getEffectiveColumn(Class<?> beanClass, ResultSetMetaData metaData, Map<String, String> columnFieldMap) throws SQLException{
		Map<String, SetterInfo> infoMap = getBeanSetterInfo(beanClass);
		
		int len = metaData.getColumnCount();
		SetterInfo[] infos = new SetterInfo[len];
		String label;
		SetterInfo info;
		for(int i=0; i<len; i++){
			label = metaData.getColumnLabel(i+1);
			info = infoMap.get(label);
			
			if(info == null && columnFieldMap != null){
				info = infoMap.get(columnFieldMap.get(label));
			}
			
			infos[i] = info;
		}
		
		return infos;
	}
	
	/**
	 * @param beanClass
	 */
	private static Map<String, SetterInfo> getBeanSetterInfo(Class<?> beanClass) {
		Map<String, SetterInfo> infoMap = setterInfoMapMap.get(beanClass);
		
		if(infoMap != null) return infoMap;

		PropertyDescriptor[] props = null;
		try {
			props = Introspector.getBeanInfo(beanClass, Object.class).getPropertyDescriptors();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		
		if(props == null || props.length == 0){
			throw new RuntimeException(beanClass.getName()+" not a bean class");
		}
		
		Map<String, SetterInfo> setterInfoMap = new HashMap<String, SetterInfo>();
		PropertyDescriptor pd;
		for(int i=0, len=props.length; i<len; i++){
			pd = props[i];
			setterInfoMap.put(pd.getDisplayName(), new SetterInfo(pd.getWriteMethod(), pd.getPropertyType()));
		}
		
		setterInfoMapMap.put(beanClass, setterInfoMap);
		
		return setterInfoMap;
	}
	
	/**/
	private static Object processColumn(ResultSet rs, int index, Class<?> fieldType) throws SQLException {
		if ( !fieldType.isPrimitive() && rs.getObject(index) == null ) {
			return null;
		}
		
		if (fieldType.equals(String.class)) {
			return rs.getString(index);
		
		} else if (fieldType.equals(Integer.TYPE) || fieldType.equals(Integer.class)) {
			return Integer.valueOf(rs.getInt(index));
		
		} else if (fieldType.equals(Boolean.TYPE) || fieldType.equals(Boolean.class)) {
			return Boolean.valueOf(rs.getBoolean(index));
		
		} else if (fieldType.equals(Long.TYPE) || fieldType.equals(Long.class)) {
			return Long.valueOf(rs.getLong(index));
		
		} else if (fieldType.equals(Double.TYPE) || fieldType.equals(Double.class)) {
			return Double.valueOf(rs.getDouble(index));
		
		} else if (fieldType.equals(Float.TYPE) || fieldType.equals(Float.class)) {
			return Float.valueOf(rs.getFloat(index));
		
		} else if (fieldType.equals(Short.TYPE) || fieldType.equals(Short.class)) {
			return Short.valueOf(rs.getShort(index));
		
		} else if(fieldType.equals(Boolean.class) || fieldType.equals(Boolean.TYPE)){
			return Boolean.valueOf(rs.getBoolean(index));
			
		} else if (fieldType.equals(Byte.TYPE) || fieldType.equals(Byte.class)) {
			return Byte.valueOf(rs.getByte(index));
		
		} else if(dataConverter != null){
			return dataConverter.ColumnToField(rs.getString(index), fieldType, rs.getMetaData().getColumnTypeName(index), index, rs);
		} else {
			return rs.getObject(index);
		}
	}

	protected static void setDataConverter(DataConverter<?> dataConverter) {
		BeanConverter.dataConverter = dataConverter;
	}
}

/**
 * @author LW
 * d
 */
class SetterInfo {
	Method method;
	Class<?> type;
	
	SetterInfo(Method method, Class<?> type){
		this.method = method;
		this.type = type;
	}
}