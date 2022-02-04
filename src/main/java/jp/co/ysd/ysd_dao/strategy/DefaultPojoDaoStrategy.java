package jp.co.ysd.ysd_dao.strategy;

import static jp.co.ysd.ysd_util.stream.StreamWrapperFactory.stream;

import java.lang.reflect.Field;
import java.util.function.Function;

import com.google.common.base.CaseFormat;

import jp.co.ysd.ysd_dao.bean.Query;
import jp.co.ysd.ysd_util.string.YsdStringUtil;

/**
 * 
 * @author yuichi
 *
 */
public class DefaultPojoDaoStrategy implements PojoDaoStrategy {

	private static final String QUERY = "SELECT * FROM %s";
	private static final String QUERY_BY_ID = QUERY + " WHERE id = :id";
	private static final String QUERY_BY_IDS = QUERY + " WHERE id IN (:ids)";
	private static final String INSERT = "INSERT INTO %s (%s) VALUES (%s)";
	private static final String UPDATE_BY_ID = "UPDATE %s SET %s WHERE id = :id";
	private static final String DELETE_BY_ID = "DELETE FROM %s WHERE id = :id";
	private static final String DELETE_BY_IDS = "DELETE FROM %s WHERE id IN (:ids)";

	@Override
	public String tableNameStrategy(Class<?> clazz) {
		String tableName = clazz.getSimpleName();
		tableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName);
		return YsdStringUtil.pluralize(tableName);
	}

	@Override
	public Query queryStrategy(Class<?> clazz) {
		return new Query(String.format(QUERY, tableNameStrategy(clazz)));
	}

	@Override
	public Query queryByIdStrategy(Class<?> clazz) {
		return new Query(String.format(QUERY_BY_ID, tableNameStrategy(clazz)));
	}

	@Override
	public Query queryByIdsStrategy(Class<?> clazz) {
		return new Query(String.format(QUERY_BY_IDS, tableNameStrategy(clazz)));
	}

	@Override
	public Query insertStrategy(Object obj) {
		String tableName = tableNameStrategy(obj.getClass());
		Function<Function<Field, String>, String> createPart = (func) -> {
			return stream(obj.getClass().getDeclaredFields()).map(func).reduce((l, r) -> l + ", " + r);
		};
		String colPart = createPart.apply((field) -> {
			return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
		});
		String paramPart = createPart.apply((field) -> {
			return ":" + field.getName();
		});
		return new Query(String.format(INSERT, tableName, colPart, paramPart));
	}

	@Override
	public Query updateByIdStrategy(Object obj) {
		String tableName = tableNameStrategy(obj.getClass());
		String colPart = stream(obj.getClass().getDeclaredFields()).filter(field -> {
			return !"id".equals(field.getName());
		}).map(field -> {
			String fieldName = field.getName();
			String colName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);
			return colName + " = :" + fieldName;
		}).reduce((l, r) -> l + ", " + r);
		return new Query(String.format(UPDATE_BY_ID, tableName, colPart));
	}

	@Override
	public Query deleteByIdStrategy(Class<?> clazz) {
		return new Query(String.format(DELETE_BY_ID, tableNameStrategy(clazz)));
	}

	@Override
	public Query deleteByIdsStrategy(Class<?> clazz) {
		return new Query(String.format(DELETE_BY_IDS, tableNameStrategy(clazz)));
	}

}
