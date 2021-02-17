package jp.co.ysd.ysd_dao.definition;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 *
 * @author yuichi
 *
 */
public class YsdDaoNamespaceHandler extends NamespaceHandlerSupport {

	@Override
	public void init() {
		registerBeanDefinitionParser("query", new QueryBeanDefinitionParser());
	}

}
