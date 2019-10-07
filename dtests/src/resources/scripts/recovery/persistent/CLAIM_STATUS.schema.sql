Create table ODS.CLAIM_STATUS(
      prsn_id bigint NOT NULL,
      clm_id bigint NOT NULL,
      seq_num INTEGER NOT NULL,
      clm_stat_id bigint GENERATED BY DEFAULT AS IDENTITY  NOT NULL,
      ver bigint NOT NULL,
      client_id bigint NOT NULL,
      clm_stat_cd LONG VARCHAR,
      cur_its_stat_cd LONG VARCHAR,
      clm_stat_ts TIMESTAMP,
      stat_rsn_ref_id bigint,
      usr_id LONG VARCHAR,
      routed_usr_id LONG VARCHAR,
      vld_frm_dt TIMESTAMP NOT NULL,
      vld_to_dt TIMESTAMP,
      src_sys_ref_id varchar(10) NOT NULL,
      src_sys_rec_id varchar(150),
      PRIMARY KEY (client_id,prsn_id,clm_id,seq_num,clm_stat_id)
      )
  USING row OPTIONS (partition_by 'prsn_id', PERSISTENCE 'sync', REDUNDANCY '1', colocate_with 'ODS.PERSONS', EVICTION_BY 'LRUHEAPPERCENT', OVERFLOW 'true', diskstore 'DISK_STORE');

create index ODS.IX_CLAIM_STATUS_01 on ODS.CLAIM_STATUS (CLIENT_ID,CLM_ID);
create index ODS.IX_CLAIM_STATUS_02 on ODS.CLAIM_STATUS (CLIENT_ID,PRSN_ID);
