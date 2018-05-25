package org.mintframework.db;


/**
 * Factory for all converters(add string support).
 * 
 * @author Michael Liao (askxuefeng@gmail.com)
 * @author LW(cnliangwei@foxmail.com)
 */
public class DataConverterProvider {
	private static DataConverter<?> converter = null;

	/**
	 * get current converter
	 * @return
	 */
	public static DataConverter<?> getConverter() {
		return converter;
	}

	/**
	 * change a converter
	 * @param converter
	 */
	public static void setConverter(DataConverter<?> converter) {
		SQLExecutor.setConverter(converter);
		BeanConverter.setDataConverter(converter);
		DataConverterProvider.converter = converter;
	}
}
