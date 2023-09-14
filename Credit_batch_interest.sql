 Procedure Credit_batch_interest(p_month_year in varchar2, --MONYYYY
                                  p_err_cd     out number,
                                  p_err_msg    out varchar2) is
    lv_from_date    date;
    lv_to_date      date;
    lv_fnyr         varchar2(8);
    lv_batch_number varchar2(30);
  begin
    p_err_cd := 0;
    IF SUBSTR(P_MONTH_YEAR, 1, 3) IN ('JAN', 'FEB', 'MAR') THEN
      lv_fnyr := to_char(to_number(SUBSTR(P_MONTH_YEAR, 4)) - 1);
    ELSE
      lv_fnyr := to_char(to_number(SUBSTR(P_MONTH_YEAR, 4)));
    END IF;
    begin
      SELECT 'CBT' || lv_fnyr || '00' ||
             nvl(max(to_number(replace(arst_batch_number,
                                       'CBT' || lv_fnyr,
                                       '')) + 1),
                 1)
        into lv_batch_number
        from gd_loyalty_summary_data
       where arst_batch_number like 'CBT' || lv_fnyr || '%';
    exception
      when no_data_found then
        lv_batch_number := 'CBT' || lv_fnyr || '001';
    end;
    lv_to_date   := to_date('21' || p_month_year, 'DDMONYYYY');
    lv_from_date := add_months(lv_to_date, -1);
    dbms_output.put_line(lv_from_date || '#' || lv_to_date);
    delete gd_loyalty_summary_data_temp
     where arst_batch_number = lv_batch_number;
    for rec in (select parent_group, dealer_map_cd, loc_cd
                  from am_dealer_loc am1
                 where exists (select 'x'
                          from gd_loyalty_trans gd1
                         where gd1.dr_parent_group = am1.parent_group
                           and gd1.dr_dlr_map_cd = am1.dealer_map_cd
                           and gd1.dr_loc_cd = am1.loc_cd
                           and gd1.trans_date >= lv_from_date
                           and gd1.trans_date < lv_to_date)
                   and am1.mul_dealer_cd <> '9967') loop
      insert into gd_loyalty_summary_data_temp
        (SOURCE,
         MUL_DEALER_CD,
         FOR_CD,
         ARST_BILL_TO_DEALER_CODE,
         ARST_BILL_TO_FOR_CODE,
         ARST_BILL_TO_OUTLET_CODE,
         ARST_CUSTOMER_CATEGORY,
         TRANS_NUM,
         TRANS_DATE,
         ARST_CURRENCY_CODE,
         ARST_CONVERSION_TYPE,
         ARST_CONVERSION_RATE,
         AMOUNT,
         ARST_REMARKS,
         ELEMENT,
         ARST_ACCOUNT_TYPE,
         ARST_FUND_VALIDATION,
         ARST_BATCH_NUMBER,
         LINE_TYPE,
         LINE_NUMBER,
         ARST_LINK_LINE,
         ARST_PROCESS_FLAG,
         ARST_PMOD_CODE,
         CARD_TYPE,
         CHANNEL,
         CREATION_MONTH,
         FIN_YEAR,
         ARST_CREATION_DATE,
         TRANSFER_FLAG,
         parent_group,
         region_cd)
      
        select /*+ use_invisible_indexes index(gd IDX_DLR_TRANS_DATE)*/
         'MUL AUTOCARD' source,
         am.mul_dealer_cd,
         am.for_cd,
         sf_get_financier_dlr_cd(am.mul_dealer_cd, am.for_cd, am.outlet_cd) ARST_BILL_TO_DEALER_CODE,
         sf_get_financier_for_cd(am.mul_dealer_cd, am.for_cd, am.outlet_cd) ARST_BILL_TO_FOR_CODE,
         sf_get_financier_outlet_cd(am.mul_dealer_cd,
                                    am.for_cd,
                                    am.outlet_cd) ARST_BILL_TO_OUTLET_CODE,
         substr(sf_get_financier_dlr_cd(am.mul_dealer_cd,
                                        am.for_cd,
                                        am.outlet_cd),
                1,
                3) ARST_CUSTOMER_CATEGORY,
         replace(lv_batch_number, 'CBT', 'C') || ar.list_code ||
         decode(am.channel, 'COM', 'C', 'EXC', 'N', 'NRM', 'A') || '-' ||
         substr(sf_get_financier_dlr_cd(am.mul_dealer_cd,
                                        am.for_cd,
                                        am.outlet_cd),
                4) || '-' ||
         sf_get_financier_outlet_cd(am.mul_dealer_cd,
                                    am.for_cd,
                                    am.outlet_cd) TRANS_NUM,
         TRUNC(SYSDATE) TRANS_DATE,
         'INR' ARST_CURRENCY_CODE,
         'USER' ARST_CONVERSION_TYPE,
         '1' ARST_CONVERSION_RATE,
         round(sum(gd.security_amount_debt), 2) AMOUNT,
         --'MARUTI AUTO CARD CREDIT AGAINST POINTS REDEEM IN ' || Commented by saket as on 12-12-2022
         DECODE(am.channel,
                'COM',
                'PRAGATI CREDIT AGAINST POINTS REDEEMED IN ',
                'MARUTI SUZUKI REWARDS CREDIT AGAINST POINTS REDEEMED IN ') || --Added by saket as on 12-12-2022
         SUBSTR(lv_to_date, 4) ARST_REMARKS,
         decode(substr(sf_get_financier_dlr_cd(am.mul_dealer_cd,
                                               am.for_cd,
                                               am.outlet_cd),
                       1,
                       3),
                'VCF',
                'AC_RRPD',
                'AC_RRPSD') ELEMENT,
         'S' ARST_ACCOUNT_TYPE,
         'N' ARST_FUND_VALIDATION,
         lv_batch_number ARST_BATCH_NUMBER,
         'LINE' LINE_TYPE,
         '1' LINE_NUMBER,
         NULL ARST_LINK_LINE,
         --'I' ARST_PROCESS_FLAG,  Commneted by saket as on 12-12-2022
         'R' ARST_PROCESS_FLAG, --Added by saket as on 12-12-2022
         'MISC' ARST_PMOD_CODE,
         AR.LIST_CODE CARD_TYPE,
         am.channel channel,
         TO_CHAR(lv_to_date, 'MON') CREATION_MONTH,
         DECODE(TO_CHAR(lv_to_date, 'MON'),
                'JAN',
                TO_CHAR(lv_to_date, 'RRRR') - 1,
                'FEB',
                TO_CHAR(lv_to_date, 'RRRR') - 1,
                'MAR',
                TO_CHAR(lv_to_date, 'RRRR') - 1,
                TO_CHAR(lv_to_date, 'RRRR')) FIN_YEAR,
         lv_to_date ARST_CREATION_DATE,
         NULL TRANSFER_FLAG,
         am.parent_group,
         am.region_cd
          from gd_loyalty_trans gd,
               am_dealer_loc    am,
               gd_loyalty_enrol le,
               AM_LIST_RANGE    AR
         where gd.dr_parent_group = am.parent_group
           and gd.dr_dlr_map_cd = am.dealer_map_cd
           and gd.dr_loc_cd = am.loc_cd
           and le.card_num = gd.card_num
           and gd.debit_pts is not null
              --and nvl(am.msil_terminated_date,sysdate)>=sysdate
           and gd.trans_date >= lv_from_date
           and gd.trans_date < lv_to_date
              --and am.mul_dealer_cd <> '9967'
           and gd.dr_parent_group = rec.parent_group
           and gd.dr_dlr_map_cd = rec.dealer_map_cd
           and gd.dr_loc_cd = rec.loc_cd
           and ar.principal_map_cd = am.principal_map_cd
           and ar.list_name = 'CARD_TYPE'
           and ar.list_code = le.card_type
           and nvl(ar.list_flag, 'Y') = 'Y'
           and ar.list_grp_code = le.channel
         GROUP BY AM.MUL_DEALER_CD,
                  AM.FOR_CD,
                  AM.OUTLET_CD,
                  'MARUTI AUTO CARD CREDIT AGAINST POINTS REDEEM IN ' ||
                  SUBSTR(lv_to_date, 4),
                  lv_batch_number,
                  AR.LIST_CODE,
                  am.channel,
                  TO_CHAR(lv_to_date, 'MON'),
                  DECODE(TO_CHAR(lv_to_date, 'MON'),
                         'JAN',
                         TO_CHAR(lv_to_date, 'RRRR') - 1,
                         'FEB',
                         TO_CHAR(lv_to_date, 'RRRR') - 1,
                         'MAR',
                         TO_CHAR(lv_to_date, 'RRRR') - 1,
                         TO_CHAR(lv_to_date, 'RRRR')),
                  am.parent_group,
                  am.region_cd;
      commit;
    
      insert into gd_loyalty_summary_data_temp
        (SOURCE,
         MUL_DEALER_CD,
         FOR_CD,
         ARST_BILL_TO_DEALER_CODE,
         ARST_BILL_TO_FOR_CODE,
         ARST_BILL_TO_OUTLET_CODE,
         ARST_CUSTOMER_CATEGORY,
         TRANS_NUM,
         TRANS_DATE,
         ARST_CURRENCY_CODE,
         ARST_CONVERSION_TYPE,
         ARST_CONVERSION_RATE,
         AMOUNT,
         ARST_REMARKS,
         ELEMENT,
         ARST_ACCOUNT_TYPE,
         ARST_FUND_VALIDATION,
         ARST_BATCH_NUMBER,
         LINE_TYPE,
         LINE_NUMBER,
         ARST_LINK_LINE,
         ARST_PROCESS_FLAG,
         ARST_PMOD_CODE,
         CARD_TYPE,
         CHANNEL,
         CREATION_MONTH,
         FIN_YEAR,
         ARST_CREATION_DATE,
         TRANSFER_FLAG,
         parent_group,
         region_cd)
      
        select /*+ use_invisible_indexes index(gd IDX_DLR_TRANS_DATE)*/
         'MUL AUTOCARD_LAD' source,
         am.mul_dealer_cd,
         am.for_cd,
         sf_get_financier_dlr_cd(am.mul_dealer_cd, am.for_cd, am.outlet_cd) ARST_BILL_TO_DEALER_CODE,
         sf_get_financier_for_cd(am.mul_dealer_cd, am.for_cd, am.outlet_cd) ARST_BILL_TO_FOR_CODE,
         sf_get_financier_outlet_cd(am.mul_dealer_cd,
                                    am.for_cd,
                                    am.outlet_cd) ARST_BILL_TO_OUTLET_CODE,
         substr(sf_get_financier_dlr_cd(am.mul_dealer_cd,
                                        am.for_cd,
                                        am.outlet_cd),
                1,
                3) ARST_CUSTOMER_CATEGORY,
         replace(lv_batch_number, 'CBT', 'C') || ar.list_code ||
         decode(am.channel, 'COM', 'C', 'EXC', 'N', 'NRM', 'A') || '-' ||
         substr(sf_get_financier_dlr_cd(am.mul_dealer_cd,
                                        am.for_cd,
                                        am.outlet_cd),
                4) || '-' ||
         sf_get_financier_outlet_cd(am.mul_dealer_cd,
                                    am.for_cd,
                                    am.outlet_cd) TRANS_NUM,
         TRUNC(SYSDATE) TRANS_DATE,
         'INR' ARST_CURRENCY_CODE,
         'USER' ARST_CONVERSION_TYPE,
         '1' ARST_CONVERSION_RATE,
         round(sum(gd.Liability_amount_debt), 2) AMOUNT,
         --'MARUTI AUTO CARD CREDIT AGAINST POINTS REDEEM IN ' || Commented by saket as on 12-12-2022
         DECODE(am.channel,
                'COM',
                'PRAGATI CREDIT AGAINST POINTS REDEEMED IN ',
                'MARUTI SUZUKI REWARDS CREDIT AGAINST POINTS REDEEMED IN ') || --Added by saket as on 12-12-2022
         SUBSTR(lv_to_date, 4) ARST_REMARKS,
         decode(substr(sf_get_financier_dlr_cd(am.mul_dealer_cd,
                                               am.for_cd,
                                               am.outlet_cd),
                       1,
                       3),
                'VCF',
                'AC_RRPD',
                'AC_RRPSD') ELEMENT,
         'S' ARST_ACCOUNT_TYPE,
         'N' ARST_FUND_VALIDATION,
         lv_batch_number ARST_BATCH_NUMBER,
         'LINE' LINE_TYPE,
         '1' LINE_NUMBER,
         NULL ARST_LINK_LINE,
         --'I' ARST_PROCESS_FLAG,  Commneted by saket as on 12-12-2022
         'R' ARST_PROCESS_FLAG, --Added by saket as on 12-12-2022
         'MISC' ARST_PMOD_CODE,
         AR.LIST_CODE CARD_TYPE,
         am.channel channel,
         TO_CHAR(lv_to_date, 'MON') CREATION_MONTH,
         DECODE(TO_CHAR(lv_to_date, 'MON'),
                'JAN',
                TO_CHAR(lv_to_date, 'RRRR') - 1,
                'FEB',
                TO_CHAR(lv_to_date, 'RRRR') - 1,
                'MAR',
                TO_CHAR(lv_to_date, 'RRRR') - 1,
                TO_CHAR(lv_to_date, 'RRRR')) FIN_YEAR,
         lv_to_date ARST_CREATION_DATE,
         NULL TRANSFER_FLAG,
         am.parent_group,
         am.region_cd
          from gd_loyalty_trans gd,
               am_dealer_loc    am,
               gd_loyalty_enrol le,
               AM_LIST_RANGE    AR
         where gd.dr_parent_group = am.parent_group
           and gd.dr_dlr_map_cd = am.dealer_map_cd
           and gd.dr_loc_cd = am.loc_cd
           and le.card_num = gd.card_num
           and gd.debit_pts is not null
              --and nvl(am.msil_terminated_date,sysdate)>=sysdate
           and gd.trans_date >= lv_from_date
           and gd.trans_date < lv_to_date
              --and am.mul_dealer_cd <> '9967'
           and gd.dr_parent_group = rec.parent_group
           and gd.dr_dlr_map_cd = rec.dealer_map_cd
           and gd.dr_loc_cd = rec.loc_cd
           and ar.principal_map_cd = am.principal_map_cd
           and ar.list_name = 'CARD_TYPE'
           and ar.list_code = le.card_type
           and nvl(ar.list_flag, 'Y') = 'Y'
           and ar.list_grp_code = le.channel
         GROUP BY AM.MUL_DEALER_CD,
                  AM.FOR_CD,
                  AM.OUTLET_CD,
                  'MARUTI AUTO CARD CREDIT AGAINST POINTS REDEEM IN ' ||
                  SUBSTR(lv_to_date, 4),
                  lv_batch_number,
                  AR.LIST_CODE,
                  am.channel,
                  TO_CHAR(lv_to_date, 'MON'),
                  DECODE(TO_CHAR(lv_to_date, 'MON'),
                         'JAN',
                         TO_CHAR(lv_to_date, 'RRRR') - 1,
                         'FEB',
                         TO_CHAR(lv_to_date, 'RRRR') - 1,
                         'MAR',
                         TO_CHAR(lv_to_date, 'RRRR') - 1,
                         TO_CHAR(lv_to_date, 'RRRR')),
                  am.parent_group,
                  am.region_cd;
      commit;
    end loop;
  
    update gd_loyalty_summary_data_temp gdd
       set GDD.ARST_BILL_TO_FOR_CODE    = '00',
           GDD.ARST_BILL_TO_OUTLET_CODE = '00',
           arst_customer_category       = 'MASS'
     where arst_customer_category = 'MAS'
       and arst_batch_number = lv_batch_number;
  
    update gd_loyalty_summary_data_temp
       set arst_bill_to_outlet_code = '000'
     where arst_batch_number = lv_batch_number
       and arst_customer_category in ('DDT', 'DMM');
  
    update gd_loyalty_summary_data_temp
       set arst_bill_to_outlet_code = '01',
           trans_num                = replace(trans_num,
                                              '-' || arst_bill_to_outlet_code,
                                              '-01')
     where arst_customer_category = 'VCF'
       and arst_batch_number = lv_batch_number
       and arst_bill_to_outlet_code <> '01';
  
    commit;
  
    DELETE FROM GD_LOYALTY_SUMMARY_DATA
     WHERE ARST_BATCH_NUMBER = LV_BATCH_NUMBER;
  
    INSERT INTO GD_LOYALTY_SUMMARY_DATA
      (SOURCE,
       ARST_BILL_TO_DEALER_CODE,
       ARST_BILL_TO_FOR_CODE,
       ARST_BILL_TO_OUTLET_CODE,
       ARST_CUSTOMER_CATEGORY,
       TRANS_NUM,
       TRANS_DATE,
       ARST_CURRENCY_CODE,
       ARST_CONVERSION_TYPE,
       ARST_CONVERSION_RATE,
       AMOUNT,
       ARST_REMARKS,
       ELEMENT,
       ARST_ACCOUNT_TYPE,
       ARST_FUND_VALIDATION,
       ARST_BATCH_NUMBER,
       LINE_TYPE,
       LINE_NUMBER,
       ARST_LINK_LINE,
       ARST_PROCESS_FLAG,
       ARST_PMOD_CODE,
       CARD_TYPE,
       CHANNEL,
       CREATION_MONTH,
       FIN_YEAR,
       ARST_CREATION_DATE,
       TRANSFER_FLAG,
       PARENT_GROUP,
       REGION_CD)
      SELECT SOURCE,
             ARST_BILL_TO_DEALER_CODE,
             ARST_BILL_TO_FOR_CODE,
             ARST_BILL_TO_OUTLET_CODE,
             ARST_CUSTOMER_CATEGORY,
             TRANS_NUM,
             TRANS_DATE,
             ARST_CURRENCY_CODE,
             ARST_CONVERSION_TYPE,
             ARST_CONVERSION_RATE,
             SUM(AMOUNT),
             ARST_REMARKS,
             ELEMENT,
             ARST_ACCOUNT_TYPE,
             ARST_FUND_VALIDATION,
             ARST_BATCH_NUMBER,
             LINE_TYPE,
             LINE_NUMBER,
             ARST_LINK_LINE,
             ARST_PROCESS_FLAG,
             ARST_PMOD_CODE,
             CARD_TYPE,
             CHANNEL,
             CREATION_MONTH,
             FIN_YEAR,
             ARST_CREATION_DATE,
             TRANSFER_FLAG,
             parent_group,
             region_cd
        FROM gd_loyalty_summary_data_temp
       WHERE ARST_BATCH_NUMBER = LV_BATCH_NUMBER
         AND NVL(AMOUNT, 0) <> 0 --Added by saket as on 17-02-2022
       GROUP BY SOURCE,
                ARST_BILL_TO_DEALER_CODE,
                ARST_BILL_TO_FOR_CODE,
                ARST_BILL_TO_OUTLET_CODE,
                ARST_CUSTOMER_CATEGORY,
                TRANS_NUM,
                TRANS_DATE,
                ARST_CURRENCY_CODE,
                ARST_CONVERSION_TYPE,
                ARST_CONVERSION_RATE,
                ARST_REMARKS,
                ELEMENT,
                ARST_ACCOUNT_TYPE,
                ARST_FUND_VALIDATION,
                ARST_BATCH_NUMBER,
                LINE_TYPE,
                LINE_NUMBER,
                ARST_LINK_LINE,
                ARST_PROCESS_FLAG,
                ARST_PMOD_CODE,
                CARD_TYPE,
                CHANNEL,
                CREATION_MONTH,
                FIN_YEAR,
                ARST_CREATION_DATE,
                TRANSFER_FLAG,
                parent_group,
                region_cd;
    --Added by saket as on 02-03-2022(start)
    DELETE FROM GD_LOYALTY_SUMMARY_DATA
     WHERE ARST_BATCH_NUMBER = LV_BATCH_NUMBER
       AND NVL(AMOUNT, 0) = 0;
    --Added by saket as on 02-03-2022(end)
    Commit;
  
    commit;
  exception
    when others then
      p_err_cd  := 1;
      p_err_msg := substr(sqlerrm, 1, 500);
  end;
