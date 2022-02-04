package jp.co.ysd.ysd_dao.strategy;

import jp.co.ysd.ysd_dao.bean.Query;

/**
 * 
 * @author yuichi
 *
 */
public interface PojoDaoStrategy {

	public String tableNameStrategy(Class<?> clazz);

	public Query queryStrategy(Class<?> clazz);

	public Query queryByIdStrategy(Class<?> clazz);

	public Query queryByIdsStrategy(Class<?> clazz);

	public Query insertStrategy(Object obj);

	public Query updateByIdStrategy(Object obj);

	public Query deleteByIdStrategy(Class<?> clazz);

	public Query deleteByIdsStrategy(Class<?> clazz);

}
