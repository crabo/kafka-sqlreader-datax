package com.nascent.pipeline.subscriber;

import java.util.Map;

import ognl.Ognl;
import ognl.OgnlException;

public final class ExpressionEvaluator {

	public static Object getValue(String expression,Map table, Object root) {
	    try {
	    	 return Ognl.getValue(expression, table,root);

	    } catch (OgnlException e) {
	      throw new RuntimeException("Error evaluating expression '" + expression + "'. Cause: " + e, e);
	    }
	}
}
