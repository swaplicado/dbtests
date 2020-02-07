/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SSmsConsts;

/**
 *
 * @author Alfredo Perez
 */
public abstract class SSmsConsts {
    
    /** Document class: income. */
    public static final String DOC_CLASS_INC = "INC";
    /** Document class: expenses. */
    public static final String DOC_CLASS_EXP = "EXP";
    /** Document type: invoice. */
    public static final String DOC_TYPE_INV = "INV";
    /** Document type: credit note. */
    public static final String DOC_TYPE_CN = "CN";
    
    /** DOCUMENT STATUS  = NEW*/
    public static final int ST_DPS_NEW = 1;
    /** DOCUMENT STATUS  = EMITED*/
    public static final int ST_DPS_EMITTED = 2;
    /** DOCUMENT STATUS  = CANCELED*/
    public static final int ST_DPS_CANCELED = 3;
}
