-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS ORDER_DETAILS;
DROP TABLE IF EXISTS staging_order_details;

-- CREATE COLUMN TABLE ORDER_DETAILS --
CREATE EXTERNAL TABLE staging_order_details USING com.databricks.spark.csv
             OPTIONS (path ':dataLocation/ORDER_DETAILS.dat', header 'true', inferSchema 'true', nullValue 'NULL');

CREATE TABLE ORDER_DETAILS USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', buckets '11', redundancy '1', EVICTION_BY ':evictionByOption') AS
             (SELECT SINGLE_ORDER_DID,SYS_ORDER_ID,SYS_ORDER_VER,DATA_SNDG_SYS_NM,SRC_SYS,SYS_PARENT_ORDER_ID,
             SYS_PARENT_ORDER_VER,PARENT_ORDER_TRD_DATE,PARENT_ORDER_SYS_NM,SYS_ALT_ORDER_ID,TRD_DATE,GIVE_UP_BROKER,
             EVENT_RCV_TS,SYS_ROOT_ORDER_ID,GLB_ROOT_ORDER_ID,GLB_ROOT_ORDER_SYS_NM,GLB_ROOT_ORDER_RCV_TS,
             SYS_ORDER_STAT_CD,SYS_ORDER_STAT_DESC_TXT,DW_STAT_CD,EVENT_TS,ORDER_OWNER_FIRM_ID,RCVD_ORDER_ID,
             EVENT_INITIATOR_ID,TRDR_SYS_LOGON_ID,SOLICITED_FG,RCVD_FROM_FIRMID_CD,RCV_DESK,SYS_ACCT_ID_SRC,
             CUST_ACCT_MNEMONIC,CUST_SLANG,SYS_ACCT_TYPE,CUST_EXCH_ACCT_ID,SYS_SECURITY_ALT_ID,TICKER_SYMBOL,
             TICKER_SYMBOL_SUFFIX,PRODUCT_CAT_CD,SIDE,LIMIT_PRICE,STOP_PRICE,ORDER_QTY,
             TOTAL_EXECUTED_QTY,AVG_PRICE,DAY_EXECUTED_QTY,DAY_AVG_PRICE,REMNG_QTY,
             CNCL_QTY,CNCL_BY_FG,EXPIRE_TS,EXEC_INSTR,TIME_IN_FORCE,RULE80AF,DEST_FIRMID_CD,
             SENT_TO_CONDUIT,SENT_TO_MPID,RCV_METHOD_CD,LIMIT_ORDER_DISP_IND,MERGED_ORDER_FG,MERGED_TO_ORDER_ID,
             RCV_DEPT_ID,ROUTE_METHOD_CD,LOCATE_ID,LOCATE_TS,LOCATE_OVERRIDE_REASON,LOCATE_BROKER,
             ORDER_BRCH_SEQ_TXT,IGNORE_CD,CLIENT_ORDER_REFID,CLIENT_ORDER_ORIG_REFID,ORDER_TYPE_CD,
             SENT_TO_ORDER_ID,ASK_PRICE,ASK_QTY,BID_PRICE,BID_QTY,REG_NMS_EXCEP_CD,
             REG_NMS_EXCEP_TXT,REG_NMS_LINK_ID,REG_NMS_PRINTS,REG_NMS_STOP_TIME,SENT_TS,RULE92,
             RULE92_OVERRIDE_TXT,RULE92_RATIO,EXMPT_STGY_BEGIN_TIME,EXMPT_STGY_END_TIME,EXMPT_STGY_PRICE_INST,
             EXMPT_STGY_QTY,CAPACITY,DISCRETION_QTY,DISCRETION_PRICE,BRCHID_CD,BASKET_ORDER_ID,
             PT_STRTGY_CD,SETL_DATE,SETL_TYPE,SETL_CURR_CD,SETL_INSTRS,COMMENT_TXT,CHANNEL_NM,
             FLOW_CAT,FLOW_CLASS,FLOW_TGT,ORDER_FLOW_ENTRY,ORDER_FLOW_CHANNEL,ORDER_FLOW_DESK,
             FLOW_SUB_CAT,STRTGY_CD,RCVD_FROM_VENDOR,RCVD_FROM_CONDUIT,SLS_PERSON_ID,SYNTHETIC_FG,
             SYNTHETIC_TYPE,FXRT,PARENT_CLREFID,REF_TIME_ID,OPT_CONTRACT_QTY,OCEAN_PRODUCT_ID,
             CREATED_BY,CREATED_DATE,FIRM_ACCT_ID,DEST,CNTRY_CD,DW_SINGLE_ORDER_CAT,CLIENT_ACCT_ID,
             EXTERNAL_TRDR_ID,ANONYMOUS_ORDER_FG,SYS_SECURITY_ALT_SRC,CURR_CD,EVENT_TYPE_CD,SYS_CLIENT_ACCT_ID,
             SYS_FIRM_ACCT_ID,SYS_TRDR_ID,DEST_ID,OPT_PUT_OR_CALL,SRC_FEED_REF_CD,DIGEST_KEY,EFF_TS,
             ENTRY_TS,OPT_STRIKE_PRICE,OPT_MATURITY_DATE,ORDER_RESTR,SHORT_SELL_EXEMPT_CD,QUOTE_TIME,
             SLS_CREDIT,SYS_SECURITY_ID,SYS_SECURITY_ID_SRC,SYS_SRC_SYS_ID,SYS_ORDER_ID_UNIQUE_SUFFIX,
             DEST_ID_SRC,GLB_ROOT_SRC_SYS_ID,GLB_ROOT_ORDER_ID_SUFFIX,SYS_ROOT_ORDER_ID_SUFFIX,SYS_PARENT_ORDER_ID_SUFFIX,
             CREDIT_BREACH_PERCENT,CREDIT_BREACH_OVERRIDE,INFO_BARRIER_ID,EXCH_PARTICIPANT_ID,REJECT_REASON_CD,
             DIRECTED_DEST,REG_NMS_LINK_TYPE,CONVER_RATIO,STOCK_REF_PRICE,CB_SWAP_ORDER_FG,EV,
             SYS_DATA_MODIFIED_TS,CMSN_TYPE,SYS_CREDIT_TRDR_ID,SYS_ENTRY_USER_ID,OPEN_CLOSE_CD,AS_OF_TRD_FG,
             HANDLING_INSTR,SECURITY_DESC,MINIMUM_QTY,CUST_OR_FIRM,MAXIMUM_SHOW,SECURITY_SUB_TYPE,
             MULTILEG_RPT_TYPE,ORDER_ACTION_TYPE,BARRIER_STYLE,AUTO_IOI_REF_TYPE,PEG_OFFSET_VAL,AUTO_IOI_OFFSET,
             IOI_PRICE,TGT_PRICE,IOI_QTY,IOI_ORDER_QTY,CMSN,SYS_LEG_REF_ID,TRADING_TYPE,
             EXCH_ORDER_ID,DEAL_ID,ORDER_TRD_TYPE,CXL_REASON FROM staging_order_details);

-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS EXEC_DETAILS;
DROP TABLE IF EXISTS staging_exec_details;

-- CREATE COLUMN TABLE EXEC_DETAILS --
CREATE EXTERNAL TABLE staging_exec_details USING com.databricks.spark.csv
             OPTIONS (path ':dataLocation/EXEC_DETAILS.dat', header 'true', inferSchema 'true', nullValue 'NULL');

CREATE TABLE EXEC_DETAILS USING column OPTIONS (partition_by 'EXEC_DID', buckets '11', redundancy '1', COLOCATE_WITH 'ORDER_DETAILS', EVICTION_BY ':evictionByOption') AS
             (SELECT EXEC_DID,SYS_EXEC_VER,SYS_EXEC_ID,TRD_DATE,ALT_EXEC_ID,SYS_EXEC_STAT,DW_EXEC_STAT,
             ORDER_OWNER_FIRM_ID,TRDR_SYS_LOGON_ID,CONTRA_BROKER_MNEMONIC,SIDE,TICKER_SYMBOL,SYS_SECURITY_ALT_ID,
             PRODUCT_CAT_CD,LAST_MKT,EXECUTED_QTY,EXEC_PRICE,EXEC_PRICE_CURR_CD,EXEC_CAPACITY,
             CLIENT_ACCT_ID,FIRM_ACCT_ID,AVG_PRICE_ACCT_ID,OCEAN_ACCT_ID,EXEC_CNTRY_CD,CMSN,COMMENT_TXT,
             ACT_BRCH_SEQ_TXT,IGNORE_CD,SRC_SYS,EXEC_TYPE_CD,LIQUIDITY_CD,ASK_PRICE,ASK_QTY,
             TRD_REPORT_ASOF_DATE,BID_PRICE,BID_QTY,CROSS_ID,NYSE_SUBREPORT_TYPE,QUOTE_COORDINATOR,
             QUOTE_TIME,REG_NMS_EXCEPT_CD,REG_NMS_EXCEPT_TXT,REG_NMS_LINK_ID,REG_NMS_MKT_CENTER_ID,REG_NMS_OVERRIDE,
             REG_NMS_PRINTS,EXECUTED_BY,TICKER_SYMBOL_SUFFIX,PREREGNMS_TRD_MOD1,PREREGNMS_TRD_MOD2,PREREGNMS_TRD_MOD3,
             PREREGNMS_TRD_MOD4,NMS_FG,GIVEUP_BROKER,CHANNEL_NM,ORDER_FLOW_ENTRY,FLOW_CAT,FLOW_CLASS,
             FLOW_TGT,ORDER_FLOW_CHANNEL,FLOW_SUBCAT,SYS_ACCT_ID_SRC,STRTGY_CD,EXECUTING_BROKER_CD,
             LEAF_EXEC_FG,RCVD_EXEC_ID,RCVD_EXEC_VER,ORDER_FLOW_DESK,SYS_ROOT_ORDER_ID,SYS_ROOT_ORDER_VER,
             GLB_ROOT_ORDER_ID,TOTAL_EXECUTED_QTY,AVG_PRICE,DEST_CD,CLIENT_ORDER_REFID,CLIENT_ORDER_ORIG_REFID,
             CROSS_EXEC_FG,OCEAN_PRODUCT_ID,TRDR_ID,REF_TIME_ID,CREATED_BY,CREATED_DATE,FIX_EXEC_ID,
             FIX_ORIGINAL_EXEC_ID,RELATED_MKT_CENTER,TRANS_TS,SYS_SECURITY_ALT_SRC,EVENT_TYPE_CD,SYS_CLIENT_ACCT_ID,
             SYS_FIRM_ACCT_ID,SYS_AVG_PRICE_ACCT_ID,SYS_TRDR_ID,ACT_BRCH_SEQ,SYS_ORDER_ID,SYS_ORDER_VER,
             SRC_FEED_REF_CD,DIGEST_KEY,TRUE_LAST_MKT,ENTRY_TS,OPT_STRIKE_PRICE,OPT_MATURITY_DATE,EXPIRE_TS,
             OPT_PUT_OR_CALL,SYS_ORDER_STAT_CD,CONTRA_ACCT,CONTRA_ACCT_SRC,CONTRA_BROKER_SRC,SYS_SECURITY_ID,
             SYS_SECURITY_ID_SRC,SYS_SRC_SYS_ID,SYS_ORDER_ID_UNIQUE_SUFFIX,DEST,DEST_ID_SRC,CONVER_RATIO,
             STOCK_REF_PRICE,AS_OF_TRD_FG,MULTILEG_RPT_TYPE,REG_NMS_LINK_TYPE,EXEC_SUB_TYPE,CMSN_TYPE,
             QUOTE_CONDITION_IND,TRD_THROUGH_FG,REGNMS_ORDER_LINK_ID,REGNMS_ORDER_LINK_TYPE,DK_IND,NBBO_QUOTE_TIME,
             GLB_ROOT_SRC_SYS_ID,TRD_REPORT_TYPE,REPORT_TO_EXCH_FG,CMPLN_COMMENT,DEAL_TYPE,EXEC_COMMENTS,
             OPTAL_FIELDS,SPOT_REF_PRICE,DELTA_OVERRIDE,UNDERLYING_PRICE,PRICE_DELTA,NORMALIZED_LIQUIDITY_IND,
             USER_AVG_PRICE,LAST_EXEC_TS,LULD_LOWER_PRICE_BAND,LULD_UPPER_PRICE_BAND,LULD_PRICE_BAND_TS,REMNG_QTY,
             ORDER_QTY,AMD_TS,SETL_CODE,SETL_DATE,CUST_NM,EXEC_TYPE,TRDR_KEY,TRDR_NM,
             FX_RATE,CUST_FX_RATE,PARENT_ORDER_SYS_NM,CNC_TYPE,FEE_AMT,FEE_CCY,BRKG_AMT,
             BRKG_CCY,CLEAR,PMT_FIX_DATE,FOLLOW_ON_FG,FX_RATE_CCY_TO,FX_RATE_CCY_FROM,CUST_FX_RATE_CCY_TO,
             CUST_FX_RATE_CCY_FROM,SYS_GFCID,CONTRA_SIDE,OPT_CONTRACT_MULTIPLIER,PRIOR_REF_PRICE_TS,SECURITY_SUB_TYPE,
             MSG_DIRECTION,LEAF_SYS_EXEC_ID,LEAF_SRC_SYS,FIX_LAST_MKT,FIX_CONTRA_BROKER_MNEMONIC,RIO_MSG_SRC,
             SNAPSHOT_TS,EXTERNAL_TRANS_TS,PRICE_CATEGORY,UNDERLYING_FX_RATE,CONVERSION_RATE,TRANS_COMMENT,
             AGGRESSOR_FLAG FROM staging_exec_details);
