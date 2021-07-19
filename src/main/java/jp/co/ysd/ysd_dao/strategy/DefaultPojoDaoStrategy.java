package jp.co.ysd.ysd_dao.strategy;

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

}
