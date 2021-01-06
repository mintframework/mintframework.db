package org.mintframework.db;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * 结果集到bean的转换器
 * @author LiangWei(cnliangwei@foxmail.com)
 * @date 2015年3月13日 下午9:41:31 
 *  
 */
public class BeanConverter {
	private static final Map<Class<?>, Map<String, SetterInfo>> setterInfoMapMap = new HashMap<Class<?>, Map<String, SetterInfo>>();
	
	private static final Pattern enumValuePattern = Pattern.compile("^\\d+$");
	
	/**
	 * 将结果集的第一列转化成bean
	 * @param beanClass 
	 * @param result
	 * @param columnFieldMap 数据库列名和bean字段名的对应关系
	 * @return
	 * @throws SQLException 
	 */
	public static <T> T toBean(Class<T> beanClass, ResultSet result, Map<String, String> columnFieldMap, FieldColumnConverter<?> converter) throws SQLException {
		T t = null;

		if(result.next()){
			t = createBean(beanClass, result, getEffectiveColumn(beanClass, result.getMetaData(), columnFieldMap), converter);
		}
		
		return t;
	}
	
	
	/**
	 * 将结果集转换成bean列表
	 * @param beanClass
	 * @param result
	 * @param columnFieldMap 数据库列名和bean字段名的对应关系
	 * @return 没有数据返回空列表
	 * @throws SQLException 
	 */
	public static <T> List<T> toBeanList(Class<T> beanClass, ResultSet result, Map<String, String> columnFieldMap, FieldColumnConverter<?> converter) throws SQLException{
		List<T> beanList = new ArrayList<T>();
		
		if(result.next()){
			SetterInfo[] infos = getEffectiveColumn(beanClass, result.getMetaData(), columnFieldMap);
			do{
				beanList.add(createBean(beanClass, result, infos, converter));
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
	private static <T> T createBean(Class<T> claz,ResultSet rslt, SetterInfo[] infos, FieldColumnConverter<?> converter) throws SQLException{
		T t = null;
		try {
			t = claz.getDeclaredConstructor().newInstance();
			SetterInfo info;
			for(int i=0,l=infos.length; i<l; i++){
				info = infos[i];
				if(info != null){
					if(info.isSetter){
						info.method.invoke(t, processColumn(rslt, i+1, info.fieldType, converter));
					} else {
						info.field.set(t, processColumn(rslt, i+1, info.fieldType, converter));
					}
				}
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
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
			label = metaData.getColumnLabel(i+1).toLowerCase();
			
			info = infoMap.get(label);
			
			if(info == null && columnFieldMap != null){
				info = infoMap.get(columnFieldMap.get(label));
			}
			
			infos[i] = info;
		}
		
		return infos;
	}
	
	/**
	 * 下划线命名风格的column也可以被转化成bean的Property
	 * @param beanClass
	 */
	private static Map<String, SetterInfo> getBeanSetterInfo(Class<?> beanClass) {
		Map<String, SetterInfo> infoMap = setterInfoMapMap.get(beanClass);
		
		if(infoMap != null) return infoMap;
		
		Map<String, SetterInfo> setterInfoMap = new HashMap<String, SetterInfo>();
		/*内省方式获取属性和setter*/
		PropertyDescriptor[] props = null;
		try {
			props = Introspector.getBeanInfo(beanClass, Object.class).getPropertyDescriptors();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		}
		
		SetterInfo setter;
		if(props != null && props.length != 0){
			PropertyDescriptor pd;
			String name;
			
			for(int i=0, len=props.length; i<len; i++){
				pd = props[i];
				
				if(pd.getWriteMethod()!=null){
					name = pd.getDisplayName();
					setter = new SetterInfo(pd.getWriteMethod(), pd.getPropertyType(), null, true);
					setterInfoMap.put(name, setter);
					
					//下划线命名风格的column也可以被转化成bean的Property
					setterInfoMap.put(camelhumpToUnderline(name), setter);
				}
			}
		}
		
		//反射获取属性，非final,static,private属性也可以注入
		for(Field f : beanClass.getFields()){
			if(Modifier.isFinal(f.getModifiers()) || Modifier.isStatic(f.getModifiers()) || Modifier.isPrivate(f.getModifiers())) continue;
			
			if(setterInfoMap.get(f.getName())!=null) continue;
			
			setter = new SetterInfo(null, f.getType(), f, false);
			
			f.setAccessible(true);
			setterInfoMap.put(f.getName(),  setter);
			
			//下划线命名风格的column也可以被转化成bean的Property
			setterInfoMap.put(camelhumpToUnderline(f.getName()), setter);
		}
		
		//缓存起来
		setterInfoMapMap.put(beanClass, setterInfoMap);
		
		return setterInfoMap;
	}
	
	 /**
	  * 将驼峰风格替换为下划线风格
     */
    private static String camelhumpToUnderline(String str) {
        Matcher matcher = Pattern.compile("[A-Z]").matcher(str);
        StringBuilder builder = new StringBuilder(str);
        for (int i = 0; matcher.find(); i++) {
            builder.replace(matcher.start() + i, matcher.end() + i, "_" + matcher.group().toLowerCase());
        }
        if (builder.charAt(0) == '_') {
            builder.deleteCharAt(0);
        }
        return builder.toString();
    }
	
	/**
	 * 
	 * @param rs
	 * @param index
	 * @param fieldType
	 * @return
	 * @throws SQLException
	 */
	private static Object processColumn(ResultSet rs, int index, Class<?> fieldType, FieldColumnConverter<?> dataConverter) throws SQLException {
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
			
		} else if(fieldType.isEnum()){
			return initEnum(rs.getString(index), fieldType);
			
		} else if(fieldType.equals(BigDecimal.class)){
			return BigDecimal.valueOf(rs.getDouble(index));
			
		} else if(dataConverter != null){
			return dataConverter.ColumnToField(rs.getString(index), fieldType, rs.getMetaData().getColumnTypeName(index));
			
		} else {
			return rs.getObject(index);
		}
	}

	/**
	 * 初始化枚举参数
	 * @param value
	 * @param enumOrdinals
	 * @param enumNames
	 * @param argType
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static Enum<?> initEnum(String value, Class<?> argType){
		Enum<?> es[] = ((Class<? extends Enum>)argType).getEnumConstants();
		if(es!=null){
			List<Integer> enumOrdinals = new ArrayList<>();
			List<String> enumNames = new ArrayList<>();
			
			for(Enum<?> e : es){
				enumOrdinals.add(e.ordinal());
				enumNames.add(e.name());
			}
			
			//索引方式初始化枚举
			if(enumValuePattern.matcher(value).matches()){
				int val = Integer.valueOf(value);
				
				if(enumOrdinals.indexOf(val)>-1){
					value = enumNames.get(val);
					return Enum.valueOf((Class<? extends Enum>)argType, value);
				}
			} else if(enumNames.indexOf(value) > -1){ //字符串方式初始化枚举
				return Enum.valueOf((Class<? extends Enum>)argType, value);
			}
		}
		
		
		return null;
	}
}

/**
 * @author LW
 * d
 */
class SetterInfo {
	public final Method 	method;
	public final Class<?> 	fieldType;
	public final Boolean 	isSetter;
	public final Field		field;
	
	SetterInfo(Method method, Class<?> type, Field field, Boolean isSetter){
		this.method = method;
		this.fieldType = type;
		this.isSetter = isSetter;
		this.field = field;
	}
}