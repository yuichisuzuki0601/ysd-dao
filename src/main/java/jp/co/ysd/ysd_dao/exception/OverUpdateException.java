package jp.co.ysd.ysd_dao.exception;

import jp.co.ysd.ysd_dao.bean.Query;

/**
 *
 * @author yuichi
 *
 */
public class OverUpdateException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private static final String MESSAGE = "The query has the risk which update more data than expects.";

	public OverUpdateException(Query query) {
		super(MESSAGE + ":" + query);
	}

}
