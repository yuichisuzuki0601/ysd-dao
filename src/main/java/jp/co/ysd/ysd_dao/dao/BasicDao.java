package jp.co.ysd.ysd_dao.dao;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import jp.co.ysd.ysd_dao.bean.Query;
import jp.co.ysd.ysd_dao.exception.OverUpdateException;
import jp.co.ysd.ysd_util.map.MapBuilder;

/**
 *
 * @author yuichi
 *
 */
@Component
public abstract class BasicDao {

	private static final String ENUM_QUERY = "SHOW COLUMNS FROM %s WHERE Field=:columnName";

	@Autowired
	private JdbcTemplate j;

	@Autowired
	protected NamedParameterJdbcTemplate nj;

	public void execute(Query query) {
		execute(this.j, query);
	}

	public void execute(JdbcTemplate j, Query query) {
		j.execute(query.getSource());
	}

	public boolean boolQuery(Query query, Map<String, Object> params) {
		return boolQuery(this.nj, query, params);
	}

	public boolean boolQuery(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> params) {
		List<Integer> res = nj.query(query.getSource(), new MapSqlParameterSource(params), (rs, i) -> rs.getInt(1));
		return !res.isEmpty() && res.get(0) == 1;
	}

	public List<String> enumQuery(String tableName, String columnName) {
		return enumQuery(this.nj, tableName, columnName);
	}

	public List<String> enumQuery(NamedParameterJdbcTemplate nj, String tableName, String columnName) {
		String query = String.format(ENUM_QUERY, tableName);
		MapBuilder param = new MapBuilder("columnName", columnName);
		List<String> res = nj.query(query, new MapSqlParameterSource(param.build()), (rs, i) -> rs.getString("Type"));
		if (!res.isEmpty()) {
			String type = res.get(0);
			if (type.startsWith("enum")) {
				return Arrays.asList(type.replaceAll("enum\\((.*?)\\)", "$1").replace("'", "").split(","));
			}
		}
		return Collections.emptyList();
	}

	public Long insert(Query query, Map<String, Object> map) {
		return insert(this.nj, query, map);
	}

	public Long insert(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> map) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		nj.update(query.getSource(), new MapSqlParameterSource(map), keyHolder, new String[] { "id" });
		Number key = keyHolder.getKey();
		return key != null ? key.longValue() : null;
	}

	public int update(Query query) {
		return update(query, null);
	}

	public int update(Query query, Map<String, Object> map) {
		return update(this.nj, query, map);
	}

	public int update(NamedParameterJdbcTemplate nj, Query query) {
		return update(nj, query, null);
	}

	public int update(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> map) {
		return nj.update(query.getSource(), map != null ? new MapSqlParameterSource(map) : null);
	}

	public boolean updateForSingle(Query query, Map<String, Object> params) {
		int successCnt = update(query, params);
		if (successCnt == 0) {
			return false;
		} else if (successCnt == 1) {
			return true;
		} else {
			throw new OverUpdateException(query);
		}
	}

	public int delete(Query query) {
		return delete(query, null);
	}

	public int delete(Query query, Map<String, Object> map) {
		return delete(this.nj, query, map);
	}

	public int delete(NamedParameterJdbcTemplate nj, Query query) {
		return delete(nj, query, null);
	}

	public int delete(NamedParameterJdbcTemplate nj, Query query, Map<String, Object> map) {
		return update(nj, query, map);
	}

	public boolean deleteById(Query query, long id) {
		return deleteById(this.nj, query, id);
	}

	public boolean deleteById(NamedParameterJdbcTemplate nj, Query query, long id) {
		int successCnt = delete(nj, query, new MapBuilder("id", id).build());
		if (successCnt == 0) {
			return false;
		} else if (successCnt == 1) {
			return true;
		} else {
			throw new OverUpdateException(query);
		}
	}

	public int deleteByIds(Query query, List<Long> ids) {
		return deleteByIds(this.nj, query, ids);
	}

	public int deleteByIds(NamedParameterJdbcTemplate nj, Query query, List<Long> ids) {
		return delete(nj, query, new MapBuilder("ids", ids).build());
	}

}
