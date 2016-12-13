package com.nascent.pipeline.processor.mysql;

import org.apache.commons.lang3.StringUtils;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Map;

public class ResultSetReadProxy {
	private static final byte[] EMPTY_CHAR_ARRAY = new byte[0];
	
	public static void transportOneRecord(Map<String,Object> record, ResultSet rs, 
			ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding) {

		try {
			for (int i = 1; i <= columnNumber; i++) {
				switch (metaData.getColumnType(i)) {

				case Types.CHAR:
				case Types.NCHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					String rawData;
					if(StringUtils.isBlank(mandatoryEncoding)){
						rawData = rs.getString(i);
					}else{
						rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : 
							rs.getBytes(i)), mandatoryEncoding);
					}
					
					record.put(metaData.getColumnName(i),rawData);
					//record.addColumn(new StringColumn(rawData));
					break;

				case Types.CLOB:
				case Types.NCLOB:
					record.put(metaData.getColumnName(i),rs.getString(i));
					break;

				case Types.SMALLINT:
				case Types.TINYINT:
				case Types.INTEGER:
				case Types.BIGINT:
					record.put(metaData.getColumnName(i),rs.getLong(i));
					//record.addColumn(new LongColumn(rs.getString(i)));
					break;

				case Types.NUMERIC:
				case Types.DECIMAL:
					record.put(metaData.getColumnName(i),rs.getDouble(i));
					//record.addColumn(new DoubleColumn(rs.getString(i)));
					break;

				case Types.FLOAT:
				case Types.REAL:
				case Types.DOUBLE:
					record.put(metaData.getColumnName(i),rs.getDouble(i));
					break;

				case Types.TIME:
					record.put(metaData.getColumnName(i),rs.getTime(i));
					break;

				// for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
				case Types.DATE:
					if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
						record.put(metaData.getColumnName(i),rs.getInt(i));
					} else {
						record.put(metaData.getColumnName(i),rs.getDate(i));
					}
					break;

				case Types.TIMESTAMP:
					record.put(metaData.getColumnName(i),rs.getTimestamp(i));
					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.BLOB:
				case Types.LONGVARBINARY:
					record.put(metaData.getColumnName(i),rs.getBytes(i));
					break;

				// warn: bit(1) -> Types.BIT 可使用BoolColumn
				// warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
				case Types.BOOLEAN:
				case Types.BIT:
					record.put(metaData.getColumnName(i),rs.getBoolean(i));
					break;

				case Types.NULL:
					String stringData = null;
					if(rs.getObject(i) != null) {
						stringData = rs.getObject(i).toString();
					}
					record.put(metaData.getColumnName(i),stringData);
					break;

				// TODO 添加BASIC_MESSAGE
				default:
					throw new RuntimeException(
									String.format(
											"您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
											metaData.getColumnName(i),
											metaData.getColumnType(i),
											metaData.getColumnClassName(i)));
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
