package com.streamsets.pipeline.lib.operation;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
public class OperationType {
	public static final String SDC_OPERATION_TYPE = "sdc.operation.type";

	  public static final int INSERT_CODE = 1;
	  public static final int DELETE_CODE = 2;
	  public static final int UPDATE_CODE = 3;
	  public static final int UPSERT_CODE = 4;
	  public static final int SELECT_FOR_UPDATE_CODE = 5;
	  public static final int BEFORE_UPDATE_CODE = 6;
	  public static final int AFTER_UPDATE_CODE = 7;

	  private static final Map<Integer, String> CODE_LABEL = new ImmutableMap.Builder<Integer, String>()
	      .put(INSERT_CODE, "INSERT")
	      .put(DELETE_CODE, "DELETE")
	      .put(UPDATE_CODE, "UPDATE")
	      .put(UPSERT_CODE, "UPSERT")
	      .put(SELECT_FOR_UPDATE_CODE, "SELECT FOR UPDATE")
	      .put(BEFORE_UPDATE_CODE, "BEFORE UPDATE")
	      .put(AFTER_UPDATE_CODE, "AFTER UPDATE")
	      .build();

	  private static final ImmutableMap<String, Integer> LABEL_CODE = new ImmutableMap.Builder<String, Integer>()
	      .put("INSERT", INSERT_CODE)
	      .put("DELETE", DELETE_CODE)
	      .put("UPDATE", UPDATE_CODE)
	      .put("UPSERT", UPSERT_CODE)
	      .put("SELECT FOR UPDATE", SELECT_FOR_UPDATE_CODE)
	      .put("BEFORE UPDATE", BEFORE_UPDATE_CODE)
	      .put("AFTER UPDATE", AFTER_UPDATE_CODE)
	      .build();


	  /**
	   * Convert from code in int type to String
	   * @param code
	   * @return
	   */
	  public static String getLabelFromIntCode(int code)  {
	    if (CODE_LABEL.containsKey(code)){
	      return CODE_LABEL.get(code);
	    }
	    return null;
	  }

	  /**
	   * Convert from code in String type to label
	   * @param code
	   * @return
	   */
	  public static String getLabelFromStringCode(String code) throws NumberFormatException {
	    try {
	      int intCode = Integer.parseInt(code);
	      return getLabelFromIntCode(intCode);
	    } catch (NumberFormatException ex) {
	      throw new NumberFormatException(
	          String.format("%s but received '%s'","operation code must be numeric", code)
	      );
	    }
	  }

	  /**
	   * Convert from label in String to Code.
	   * @param op
	   * @return int value of the code. -1 if not defined.
	   */
	  public static int getCodeFromLabel(String op) {
	    if (LABEL_CODE.containsKey(op)) {
	      return LABEL_CODE.get(op);
	    }
	    return -1;
	  }
}
