DROP TABLE IF EXISTS ADJUSTMENT;
CREATE TABLE IF NOT EXISTS ADJUSTMENT(BILL_ENT_ID BIGINT NOT NULL,
ADJ_ID BIGINT NOT NULL,
VER BIGINT NOT NULL,
CLIENT_ID BIGINT NOT NULL,
ADJ_TYP_REF_ID BIGINT NOT NULL,
IS_CR_ADJ INT,
BILL_ENT_SCHD_ID BIGINT NOT NULL,
ADJ_AMT NUMERIC(18,4) NOT NULL,
DESCR VARCHAR(50),
POST_DT DATE,
ORIG_RCPT_ID BIGINT,
NSF_RSN_REF_ID BIGINT,
WHLD_TYP_REF_ID BIGINT,
SRC_TYP_REF_ID BIGINT,
BILL_SRC_ID BIGINT,
VLD_FRM_DT TIMESTAMP NOT NULL,
VLD_TO_DT TIMESTAMP,
SRC_SYS_REF_ID VARCHAR(10) NOT NULL,
SRC_SYS_REC_ID VARCHAR(150)) USING column OPTIONS(partition_by 'BILL_ENT_ID',buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,ADJ_ID' ) ;
deploy jar jdbc1 ':dataLocation';
deploy package MSSQL 'com.microsoft.sqlserver:sqljdbc4:4.0' repos 'http://clojars.org/repo/' path ':homeDirLocation/work/sqljdbcJar';