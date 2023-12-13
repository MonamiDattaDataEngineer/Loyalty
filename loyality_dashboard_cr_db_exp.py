
gd_loyalty_trans_date_DF = gd_loyalty_trans_DF.filter(col('TRANS_DATE') >= '2020-05-21')
gd_loyalty_trans_exp_date_DF = gd_loyalty_trans_DF.filter(col('EXPIRY_DATE') >= '2020-05-21')

for_cd_DF1 = am_dealer_loc_DF.select('MUL_DEALER_CD','FOR_CD','OUTLET_CD',col('PARENT_GROUP').alias('LV_PARENT_GROUP'),'DEALER_CATEGORY').distinct()

#outlet_cd_DF1 = am_dealer_loc_DF.select('MUL_DEALER_CD','FOR_CD','OUTLET_CD',col('PARENT_GROUP').alias('LV_PARENT_GROUP'),'DEALER_CATEGORY').distinct()
#delr_cd_DF1 = am_dealer_loc_DF.select('MUL_DEALER_CD','FOR_CD','OUTLET_CD',col('PARENT_GROUP').alias('LV_PARENT_GROUP'),'DEALER_CATEGORY').distinct()
for_cd_DF2 = for_cd_DF1.withColumn('LV_FOR_CD', when(col('DEALER_CATEGORY') == 'MAS', col("FOR_CD")))
for_cd_DF2 = for_cd_DF2.withColumn('LV_DLR_CD', when(col('DEALER_CATEGORY') == 'MAS', concat(lit('MASS'),col('MUL_DEALER_CD'))))
for_cd_DF2 =for_cd_DF2.withColumn('LV_OUTLET_CD', when(col('DEALER_CATEGORY') == 'MAS', col("OUTLET_CD")))

for_cd_DF2.filter(col('DEALER_CATEGORY') == 'MAS').show(5)
for_cd_DF3 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()

for_cd_DF4 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()

for_cd_DF5 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()

for_cd_DF6 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()

for_cd_DF7 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF8 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF9 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF10 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF11 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF12 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF13 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF14 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()


interim_join_1 = for_cd_DF1.join(dmsd_ew_fin_DF, (dmsd_ew_fin_DF.DEALER_CD == for_cd_DF1.MUL_DEALER_CD) 
                                    & (dmsd_ew_fin_DF.FOR_CD == for_cd_DF1.FOR_CD) 
                                    & (dmsd_ew_fin_DF.OUTLET_CD == for_cd_DF1.OUTLET_CD) 
                                    ,'inner')\
                            .where(col('LV_PARENT_GROUP').isNotNull() 
                                  & col('FINANCIER_DELR_CATG').isin(['VCF','DDL','DMM','MUL']))\
                            .select('DEALER_CD',dmsd_ew_fin_DF.FOR_CD,dmsd_ew_fin_DF.OUTLET_CD,'FINANCIER_DELR_CATG','LV_PARENT_GROUP',
                                    col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_outlet_CD').alias('LV_OUTLET_CD') )\
                            .distinct()
#Finding Null for lv_for_cd for df2 and df3

#naming for_cd as fortemp for df3 
for_cd_DF = for_cd_DF2.join(for_cd_DF3,((for_cd_DF2.MUL_DEALER_CD == for_cd_DF3.DEALER_CD ) 
                              & (for_cd_DF2.FOR_CD == for_cd_DF3.FOR_CD ) 
                              & (for_cd_DF2.OUTLET_CD == for_cd_DF3.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF2.MUL_DEALER_CD, for_cd_DF2.FOR_CD , for_cd_DF2.OUTLET_CD, for_cd_DF2.DEALER_CATEGORY,for_cd_DF2.LV_PARENT_GROUP, for_cd_DF2.LV_FOR_CD, for_cd_DF3.LV_FOR_CD.alias('fortemp'),for_cd_DF2.LV_DLR_CD,for_cd_DF3.LV_DLR_CD.alias('temp'), for_cd_DF2.LV_OUTLET_CD, for_cd_DF3.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if df2_forrd is null then df3_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                       .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df4

#naming for_cd as fortemp for df4
for_cd_DF = for_cd_DF.join(for_cd_DF4,((for_cd_DF.MUL_DEALER_CD == for_cd_DF4.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF4.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF4.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF4.LV_FOR_CD.alias('fortemp'),for_cd_DF4.LV_DLR_CD.alias('temp'),for_cd_DF4.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df4_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df5

#naming for_cd as fortemp for df5
for_cd_DF = for_cd_DF.join(for_cd_DF5,((for_cd_DF.MUL_DEALER_CD == for_cd_DF5.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF5.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF5.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF5.LV_FOR_CD.alias('fortemp'),for_cd_DF5.LV_DLR_CD.alias('temp'),for_cd_DF5.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df5_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                     .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                     .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                     .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df6

#naming for_cd as fortemp for df5
for_cd_DF = for_cd_DF.join(for_cd_DF6,((for_cd_DF.MUL_DEALER_CD == for_cd_DF6.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF6.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF6.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF6.LV_FOR_CD.alias('fortemp'),for_cd_DF6.LV_DLR_CD.alias('temp'),for_cd_DF6.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df6_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df7

#naming for_cd as fortemp for df7
for_cd_DF = for_cd_DF.join(for_cd_DF7,((for_cd_DF.MUL_DEALER_CD == for_cd_DF7.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF7.FOR_CD ) ) 
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF7.LV_FOR_CD.alias('fortemp'),for_cd_DF7.LV_DLR_CD.alias('temp'),for_cd_DF7.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df7_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df8

#naming for_cd as fortemp for df8
for_cd_DF = for_cd_DF.join(for_cd_DF8,((for_cd_DF.MUL_DEALER_CD == for_cd_DF8.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF8.FOR_CD ) ) 
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF8.LV_FOR_CD.alias('fortemp'),for_cd_DF8.LV_DLR_CD.alias('temp'),for_cd_DF8.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df8_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select(col('MUL_DEALER_CD').alias('MUL_DEALER_CD1'),
                                col('FOR_CD').alias('FOR_CD1'),
                                col('OUTLET_CD').alias('OUTLET_CD1'),
                                col('DEALER_CATEGORY').alias('DEALER_CATEGORY1'), 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')

# for_cd_DF.show(5)
am_dealer_loc_DF = am_dealer_loc_DF.filter(col('MUL_DEALER_CD') != '9967' )

# res_1 has all data for credit dataset
res_1 = am_dealer_loc_DF.join(gd_loyalty_trans_DF,((gd_loyalty_trans_DF.DR_PARENT_GROUP == am_dealer_loc_DF.PARENT_GROUP)\
							& (gd_loyalty_trans_DF.DR_DLR_MAP_CD == am_dealer_loc_DF.DEALER_MAP_CD)\
							& (gd_loyalty_trans_DF.DR_LOC_CD == am_dealer_loc_DF.LOC_CD)\
							), 'inner')\
						.where((col('MUL_DEALER_CD') != '9967' ) & (col('DEBIT_PTS').isNotNull()))
						

res_1 = res_1.join(gd_loyalty_enrol_DF,(gd_loyalty_enrol_DF.CARD_NUM == res_1.CARD_NUM), 'inner')

res_1 = res_1.join(am_list_range_DF,(am_list_range_DF.PRINCIPAL_MAP_CD == res_1.PRINCIPAL_MAP_CD)\
							& (gd_loyalty_enrol_DF.CARD_TYPE == am_list_range_DF.LIST_CODE)\
							& (gd_loyalty_enrol_DF.CHANNEL == am_list_range_DF.LIST_GRP_CODE)\
							, 'inner')\
			 .where(((col('LIST_FLAG').isNull()) | (col('LIST_FLAG') == 'Y'))  \
                           & (col('LIST_NAME') == 'CARD_TYPE')) 
res_1 = res_1.select(
                lit('MUL AUTOCARD').alias('SOURCE') ,
                col('MUL_DEALER_CD').alias('MUL_DEALER_CD') ,
                col('FOR_CD').alias('FOR_CD') ,
                col('OUTLET_CD').alias('OUTLET_CD') ,
                col('DEALER_CATEGORY').alias('DEALER_CATEGORY') ,                
                lit(None).alias('ARST_BILL_TO_DEALER_CODE') ,
                lit(None).alias('ARST_BILL_TO_FOR_CODE') ,
                lit(None).alias('ARST_BILL_TO_OUTLET_CODE') ,
                lit(None).alias('ARST_CUSTOMER_CATEGORY') ,
                lit(None).alias('TRANS_NUM') ,
                col('TRANS_DATE').alias('TRANS_DATE'),
                lit(date.today()).alias('CURRENT_DATE') ,
                lit('INR').alias('ARST_CURRENCY_CODE') ,
                lit('USER').alias('ARST_CONVERSION_TYPE') ,
                lit('1').alias('ARST_CONVERSION_RATE') ,
                col('SECURITY_AMOUNT_DEBT') ,
                col('LIABILITY_AMOUNT_DEBT'),
                lit(None).alias('ARST_REMARKS') ,
                lit(None).alias('ELEMENT') ,
                lit('S').alias('ARST_ACCOUNT_TYPE') ,
                lit('N').alias('ARST_FUND_VALIDATION') ,
                lit(None).alias('ARST_BATCH_NUMBER') ,
                lit('LINE').alias('LINE_TYPE') ,
                lit('1').alias('LINE_NUMBER') ,
                lit(None).alias('ARST_LINK_LINE') ,
                lit('R').alias('ARST_PROCESS_FLAG') ,
                lit('MISC').alias('ARST_PMOD_CODE') ,
                col('LIST_CODE').alias('CARD_TYPE') ,
                am_dealer_loc_DF.CHANNEL.alias('CHANNEL') ,
                lit(None).alias('CREATION_MONTH') ,
                lit(None).alias('FIN_YEAR') ,
                lit(None).alias('ARST_CREATION_DATE') ,
                lit(None).alias('TRANSFER_FLAG') ,
                col('PARENT_GROUP').alias('PARENT_GROUP') ,
                col('REGION_CD').alias('REGION_CD'))
# res_1.show(5)
res_1= res_1.join(for_cd_DF, (( res_1.MUL_DEALER_CD == for_cd_DF.MUL_DEALER_CD1 )\
                             & ( res_1.FOR_CD == for_cd_DF.FOR_CD1  )\
                             & ( res_1.OUTLET_CD == for_cd_DF.OUTLET_CD1 )), 'left')
res_1 = res_1.select(col('SOURCE') ,
                    col('MUL_DEALER_CD') ,
                    col('FOR_CD') ,
                    col('ARST_BILL_TO_DEALER_CODE') ,
                    col('ARST_BILL_TO_FOR_CODE') ,
                    col('ARST_BILL_TO_OUTLET_CODE') ,
                    col('ARST_CUSTOMER_CATEGORY') ,
                    col('TRANS_NUM') ,
                    col('TRANS_DATE') ,
                    col('CURRENT_DATE'),
                    col('ARST_CURRENCY_CODE') ,
                    col('ARST_CONVERSION_TYPE') ,
                    col('ARST_CONVERSION_RATE') ,
                    col('SECURITY_AMOUNT_DEBT') ,
                    col('LIABILITY_AMOUNT_DEBT'),
                    col('ARST_REMARKS') ,
                    col('ELEMENT') ,
                    col('ARST_ACCOUNT_TYPE') ,
                    col('ARST_FUND_VALIDATION') ,
                    col('ARST_BATCH_NUMBER') ,
                    col('LINE_TYPE') ,
                    col('LINE_NUMBER') ,
                    col('ARST_LINK_LINE') ,
                    col('ARST_PROCESS_FLAG') ,
                    col('ARST_PMOD_CODE') ,
                    col('CARD_TYPE') ,
                    col('CHANNEL') ,
                    col('CREATION_MONTH') ,
                    col('FIN_YEAR') ,
                    col('ARST_CREATION_DATE') ,
                    col('TRANSFER_FLAG') ,
                    col('PARENT_GROUP') ,
                    col('REGION_CD') ,
                    col('LV_PARENT_GROUP') ,
                    col('LV_FOR_CD') ,
                    col('LV_DLR_CD') ,
                    col('LV_OUTLET_CD') 
)
# res_1.show(5)
res_1 = res_1.withColumn('ARST_BILL_TO_DEALER_CODE',col('LV_DLR_CD') )\
            .withColumn('ARST_BILL_TO_FOR_CODE',col('LV_FOR_CD') )\
            .withColumn('ARST_BILL_TO_OUTLET_CODE',col('LV_OUTLET_CD') )\
            .withColumn('ARST_CUSTOMER_CATEGORY',substring(col('LV_DLR_CD'),1,3) )
res_1 = res_1.withColumn('ELEMENT',when(substring(col('LV_DLR_CD'),1,3) == 'VCF', 'AC_RRPD').otherwise('AC_RRPSD') )
# res_1.show(5)
print('ji')
res_1 = res_1.withColumn('FINANCIAL_YEAR',when(month('TRANS_DATE') < 4, year('TRANS_DATE')-1).otherwise(year('TRANS_DATE')))\
            .withColumn('FINANCIAL_MONTH',when(month('TRANS_DATE') < 4, concat(lit('0'),month('TRANS_DATE') + 9)).otherwise(concat(lit('00'),month('TRANS_DATE') - 3)))\
            .withColumn('LV_BATCH_NUMBER',concat(lit('CBT'),col('FINANCIAL_YEAR'),col('FINANCIAL_MONTH')))
res_1 = res_1.withColumn('TRANS_NUM',when(col('CHANNEL') == 'COM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'CBT', 'C'),col('CARD_TYPE'),lit('C-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'EXC' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'CBT', 'C'),col('CARD_TYPE'),lit('N-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'NRM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'CBT', 'C'),col('CARD_TYPE'),lit('A-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE'))))

res_1 = res_1.withColumn('ARST_REMARKS', when(col('CHANNEL') == 'COM' ,concat(lit('PRAGATI CREDIT AGAINST POINTS REDEEMED IN '),  date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR')) )\
                        .otherwise(concat(lit('MARUTI SUZUKI REWARDS CREDIT AGAINST POINTS REDEEMED IN '), date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR'))))
res_1 = res_1.withColumn('CREATION_MONTH', date_format('TRANS_DATE', 'MMM'))\
             .withColumn('FIN_YEAR', col('FINANCIAL_YEAR'))\
             .withColumn('ARST_BATCH_NUMBER', col('LV_BATCH_NUMBER'))\
             .withColumn('ARST_CREATION_DATE', last_day('TRANS_DATE'))
# res_1.show(5, truncate = False)
res_1.columns
group_by_list = ['SOURCE', 'MUL_DEALER_CD', 'FOR_CD', 'ARST_BILL_TO_DEALER_CODE', 'ARST_BILL_TO_FOR_CODE', 'ARST_BILL_TO_OUTLET_CODE', 'ARST_CUSTOMER_CATEGORY', 'TRANS_NUM',  
                 'ARST_CURRENCY_CODE', 'ARST_CONVERSION_TYPE', 'ARST_CONVERSION_RATE',  'ARST_REMARKS', 'ELEMENT', 'ARST_ACCOUNT_TYPE', 'ARST_FUND_VALIDATION', 'ARST_BATCH_NUMBER', 
                 'LINE_TYPE', 'LINE_NUMBER', 'ARST_LINK_LINE', 'ARST_PROCESS_FLAG', 'ARST_PMOD_CODE', 'CARD_TYPE', 'CHANNEL', 'CREATION_MONTH', 'FIN_YEAR', 'ARST_CREATION_DATE', 
                 'TRANSFER_FLAG', 'PARENT_GROUP', 'REGION_CD']

res_1_amount_DF = res_1.groupBy(group_by_list).agg(sum('SECURITY_AMOUNT_DEBT').alias('AMOUNT1'), sum('LIABILITY_AMOUNT_DEBT').alias('AMOUNT2'))
res_1_amount_DF = res_1_amount_DF.withColumn('AMOUNT', col('AMOUNT1')+col('AMOUNT2'))
# res_1_amount_DF.filter(col('TRANS_NUM') == 'C2023004CA-0117-01').show()
#Code for Debit interest, it will have 2 data sets , res_2 and res_3 
res_debit = am_dealer_loc_DF.join(gd_loyalty_trans_DF,((gd_loyalty_trans_DF.TRANS_PARENT_GROUP == am_dealer_loc_DF.PARENT_GROUP)\
							& (gd_loyalty_trans_DF.TRANS_DLR_MAP_CD == am_dealer_loc_DF.DEALER_MAP_CD)\
							& (gd_loyalty_trans_DF.TRANS_LOC_CD == am_dealer_loc_DF.LOC_CD)\
							), 'inner')\
						.where((col('MUL_DEALER_CD') != '9967' ))

res_debit = res_debit.join(gd_loyalty_enrol_DF,(gd_loyalty_enrol_DF.CARD_NUM == res_debit.CARD_NUM), 'inner')

res_debit = res_debit.join(am_list_range_DF,(am_list_range_DF.PRINCIPAL_MAP_CD == res_debit.PRINCIPAL_MAP_CD)\
							& (gd_loyalty_enrol_DF.CARD_TYPE == am_list_range_DF.LIST_CODE)\
							& (gd_loyalty_enrol_DF.CHANNEL == am_list_range_DF.LIST_GRP_CODE)\
							, 'inner')\
			 .where(((col('LIST_FLAG').isNull()) | (col('LIST_FLAG') == 'Y')) \
                           & (col('LIST_NAME') == 'CARD_TYPE') ) 
res_2 = res_debit.select(
                lit('MUL AUTOCARD').alias('SOURCE') ,
                col('MUL_DEALER_CD').alias('MUL_DEALER_CD') ,
                col('FOR_CD').alias('FOR_CD') ,
                col('OUTLET_CD').alias('OUTLET_CD') ,
                col('DEALER_CATEGORY').alias('DEALER_CATEGORY') ,                
                lit(None).alias('ARST_BILL_TO_DEALER_CODE') ,
                lit(None).alias('ARST_BILL_TO_FOR_CODE') ,
                lit(None).alias('ARST_BILL_TO_OUTLET_CODE') ,
                lit(None).alias('ARST_CUSTOMER_CATEGORY') ,
                lit(None).alias('TRANS_NUM') ,
                col('TRANS_DATE').alias('TRANS_DATE'),
                lit(date.today()).alias('CURRENT_DATE') ,
                lit('INR').alias('ARST_CURRENCY_CODE') ,
                lit('USER').alias('ARST_CONVERSION_TYPE') ,
                lit('1').alias('ARST_CONVERSION_RATE') ,
                col('SECURITY_AMOUNT') ,
                lit(None).alias('ARST_REMARKS') ,
                lit(None).alias('ELEMENT') ,
                lit('S').alias('ARST_ACCOUNT_TYPE') ,
                lit('N').alias('ARST_FUND_VALIDATION') ,
                lit(None).alias('ARST_BATCH_NUMBER') ,
                lit('LINE').alias('LINE_TYPE') ,
                lit('1').alias('LINE_NUMBER') ,
                lit(None).alias('ARST_LINK_LINE') ,
                lit('R').alias('ARST_PROCESS_FLAG') ,
                lit('MISC').alias('ARST_PMOD_CODE') ,
                col('LIST_CODE').alias('CARD_TYPE') ,
                am_dealer_loc_DF.CHANNEL.alias('CHANNEL') ,
                lit(None).alias('CREATION_MONTH') ,
                lit(None).alias('FIN_YEAR') ,
                lit(None).alias('ARST_CREATION_DATE') ,
                lit(None).alias('TRANSFER_FLAG') ,
                col('PARENT_GROUP').alias('PARENT_GROUP') ,
                col('REGION_CD').alias('REGION_CD'))
res_3 = res_debit.select(
                lit('MUL AUTOCARD_LAD').alias('SOURCE') ,
                col('MUL_DEALER_CD').alias('MUL_DEALER_CD') ,
                col('FOR_CD').alias('FOR_CD') ,
                col('OUTLET_CD').alias('OUTLET_CD') ,
                col('DEALER_CATEGORY').alias('DEALER_CATEGORY') ,                
                lit(None).alias('ARST_BILL_TO_DEALER_CODE') ,
                lit(None).alias('ARST_BILL_TO_FOR_CODE') ,
                lit(None).alias('ARST_BILL_TO_OUTLET_CODE') ,
                lit(None).alias('ARST_CUSTOMER_CATEGORY') ,
                lit(None).alias('TRANS_NUM') ,
                col('TRANS_DATE').alias('TRANS_DATE'),
                lit(date.today()).alias('CURRENT_DATE') ,
                lit('INR').alias('ARST_CURRENCY_CODE') ,
                lit('USER').alias('ARST_CONVERSION_TYPE') ,
                lit('1').alias('ARST_CONVERSION_RATE') ,
                col('LIABILITY_AMOUNT_DEBT') ,
                lit(None).alias('ARST_REMARKS') ,
                lit(None).alias('ELEMENT') ,
                lit('S').alias('ARST_ACCOUNT_TYPE') ,
                lit('N').alias('ARST_FUND_VALIDATION') ,
                lit(None).alias('ARST_BATCH_NUMBER') ,
                lit('LINE').alias('LINE_TYPE') ,
                lit('1').alias('LINE_NUMBER') ,
                lit(None).alias('ARST_LINK_LINE') ,
                lit('R').alias('ARST_PROCESS_FLAG') ,
                lit('MISC').alias('ARST_PMOD_CODE') ,
                col('LIST_CODE').alias('CARD_TYPE') ,
                am_dealer_loc_DF.CHANNEL.alias('CHANNEL') ,
                lit(None).alias('CREATION_MONTH') ,
                lit(None).alias('FIN_YEAR') ,
                lit(None).alias('ARST_CREATION_DATE') ,
                lit(None).alias('TRANSFER_FLAG') ,
                col('PARENT_GROUP').alias('PARENT_GROUP') ,
                col('REGION_CD').alias('REGION_CD'))
res_2 = res_2.join(for_cd_DF, (( res_2.MUL_DEALER_CD == for_cd_DF.MUL_DEALER_CD1 )\
                             & ( res_2.FOR_CD == for_cd_DF.FOR_CD1  )\
                             & ( res_2.OUTLET_CD == for_cd_DF.OUTLET_CD1 )), 'left')

res_3 = res_3.join(for_cd_DF, (( res_3.MUL_DEALER_CD == for_cd_DF.MUL_DEALER_CD1 )\
                             & ( res_3.FOR_CD == for_cd_DF.FOR_CD1  )\
                             & ( res_3.OUTLET_CD == for_cd_DF.OUTLET_CD1 )), 'left')
res_2 = res_2.select(col('SOURCE') ,
                    col('MUL_DEALER_CD') ,
                    col('FOR_CD') ,
                    col('ARST_BILL_TO_DEALER_CODE') ,
                    col('ARST_BILL_TO_FOR_CODE') ,
                    col('ARST_BILL_TO_OUTLET_CODE') ,
                    col('ARST_CUSTOMER_CATEGORY') ,
                    col('TRANS_NUM') ,
                    col('TRANS_DATE') ,
                    col('CURRENT_DATE'),
                    col('ARST_CURRENCY_CODE') ,
                    col('ARST_CONVERSION_TYPE') ,
                    col('ARST_CONVERSION_RATE') ,
                    col('SECURITY_AMOUNT') ,
                    col('ARST_REMARKS') ,
                    col('ELEMENT') ,
                    col('ARST_ACCOUNT_TYPE') ,
                    col('ARST_FUND_VALIDATION') ,
                    col('ARST_BATCH_NUMBER') ,
                    col('LINE_TYPE') ,
                    col('LINE_NUMBER') ,
                    col('ARST_LINK_LINE') ,
                    col('ARST_PROCESS_FLAG') ,
                    col('ARST_PMOD_CODE') ,
                    col('CARD_TYPE') ,
                    col('CHANNEL') ,
                    col('CREATION_MONTH') ,
                    col('FIN_YEAR') ,
                    col('ARST_CREATION_DATE') ,
                    col('TRANSFER_FLAG') ,
                    col('PARENT_GROUP') ,
                    col('REGION_CD') ,
                    col('LV_PARENT_GROUP') ,
                    col('LV_FOR_CD') ,
                    col('LV_DLR_CD') ,
                    col('LV_OUTLET_CD') 
)
res_3 = res_3.select(col('SOURCE') ,
                    col('MUL_DEALER_CD') ,
                    col('FOR_CD') ,
                    col('ARST_BILL_TO_DEALER_CODE') ,
                    col('ARST_BILL_TO_FOR_CODE') ,
                    col('ARST_BILL_TO_OUTLET_CODE') ,
                    col('ARST_CUSTOMER_CATEGORY') ,
                    col('TRANS_NUM') ,
                    col('TRANS_DATE') ,
                    col('CURRENT_DATE'),
                    col('ARST_CURRENCY_CODE') ,
                    col('ARST_CONVERSION_TYPE') ,
                    col('ARST_CONVERSION_RATE') ,
                    col('LIABILITY_AMOUNT_DEBT'),
                    col('ARST_REMARKS') ,
                    col('ELEMENT') ,
                    col('ARST_ACCOUNT_TYPE') ,
                    col('ARST_FUND_VALIDATION') ,
                    col('ARST_BATCH_NUMBER') ,
                    col('LINE_TYPE') ,
                    col('LINE_NUMBER') ,
                    col('ARST_LINK_LINE') ,
                    col('ARST_PROCESS_FLAG') ,
                    col('ARST_PMOD_CODE') ,
                    col('CARD_TYPE') ,
                    col('CHANNEL') ,
                    col('CREATION_MONTH') ,
                    col('FIN_YEAR') ,
                    col('ARST_CREATION_DATE') ,
                    col('TRANSFER_FLAG') ,
                    col('PARENT_GROUP') ,
                    col('REGION_CD') ,
                    col('LV_PARENT_GROUP') ,
                    col('LV_FOR_CD') ,
                    col('LV_DLR_CD') ,
                    col('LV_OUTLET_CD') 
)

res_2 = res_2.withColumn('ARST_BILL_TO_DEALER_CODE',col('LV_DLR_CD') )\
            .withColumn('ARST_BILL_TO_FOR_CODE',col('LV_FOR_CD') )\
            .withColumn('ARST_BILL_TO_OUTLET_CODE',col('LV_OUTLET_CD') )\
            .withColumn('ARST_CUSTOMER_CATEGORY',substring(col('LV_DLR_CD'),1,3) )
res_2 = res_2.withColumn('ELEMENT',when(substring(col('LV_DLR_CD'),1,3) == 'VCF', 'AC_RPDT').otherwise('AC_RPSDT') )

res_2 = res_2.withColumn('FINANCIAL_YEAR',when(month('TRANS_DATE') < 4, year('TRANS_DATE')-1).otherwise(year('TRANS_DATE')))\
            .withColumn('FINANCIAL_MONTH',when(month('TRANS_DATE') < 4, concat(lit('0'),month('TRANS_DATE') + 9)).otherwise(concat(lit('00'),month('TRANS_DATE') - 3)))\
            .withColumn('LV_BATCH_NUMBER',concat(lit('DBT'),col('FINANCIAL_YEAR'),col('FINANCIAL_MONTH')))

res_2 = res_2.withColumn('TRANS_NUM',when(col('CHANNEL') == 'COM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'DBT', 'D'),col('CARD_TYPE'),lit('C-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'EXC' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'DBT', 'D'),col('CARD_TYPE'),lit('N-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'NRM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'DBT', 'D'),col('CARD_TYPE'),lit('A-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE'))))

res_2 = res_2.withColumn('ARST_REMARKS', when(col('CHANNEL') == 'COM' ,concat(lit('PRAGATI DEBIT AGAINST POINTS AWARDED IN '),  date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR')) )\
                        .otherwise(concat(lit('MARUTI SUZUKI REWARDS DEBIT AGAINST POINTS AWARDED IN '), date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR'))))

res_2 = res_2.withColumn('CREATION_MONTH', date_format('TRANS_DATE', 'MMM'))\
             .withColumn('FIN_YEAR', col('FINANCIAL_YEAR'))\
             .withColumn('ARST_BATCH_NUMBER', col('LV_BATCH_NUMBER'))\
             .withColumn('ARST_CREATION_DATE', last_day('TRANS_DATE'))						

res_3 = res_3.withColumn('ARST_BILL_TO_DEALER_CODE',col('LV_DLR_CD') )\
            .withColumn('ARST_BILL_TO_FOR_CODE',col('LV_FOR_CD') )\
            .withColumn('ARST_BILL_TO_OUTLET_CODE',col('LV_OUTLET_CD') )\
            .withColumn('ARST_CUSTOMER_CATEGORY',substring(col('LV_DLR_CD'),1,3) )
res_3 = res_3.withColumn('ELEMENT',when(substring(col('LV_DLR_CD'),1,3) == 'VCF', 'AC_RPDT').otherwise('AC_RPSDT') )

res_3 = res_3.withColumn('FINANCIAL_YEAR',when(month('TRANS_DATE') < 4, year('TRANS_DATE')-1).otherwise(year('TRANS_DATE')))\
            .withColumn('FINANCIAL_MONTH',when(month('TRANS_DATE') < 4, concat(lit('0'),month('TRANS_DATE') + 9)).otherwise(concat(lit('00'),month('TRANS_DATE') - 3)))\
            .withColumn('LV_BATCH_NUMBER',concat(lit('DBT'),col('FINANCIAL_YEAR'),col('FINANCIAL_MONTH')))

res_3 = res_3.withColumn('TRANS_NUM',when(col('CHANNEL') == 'COM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'DBT', 'D'),col('CARD_TYPE'),lit('C-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'EXC' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'DBT', 'D'),col('CARD_TYPE'),lit('N-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'NRM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'DBT', 'D'),col('CARD_TYPE'),lit('A-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE'))))

res_3 = res_3.withColumn('ARST_REMARKS', when(col('CHANNEL') == 'COM' ,concat(lit('PRAGATI DEBIT AGAINST POINTS AWARDED IN '), date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR')) )\
                        .otherwise(concat(lit('MARUTI SUZUKI REWARDS DEBIT AGAINST POINTS AWARDED IN '), date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR'))))

res_3 = res_3.withColumn('CREATION_MONTH', date_format('TRANS_DATE', 'MMM'))\
             .withColumn('FIN_YEAR', col('FINANCIAL_YEAR'))\
             .withColumn('ARST_BATCH_NUMBER', col('LV_BATCH_NUMBER'))\
             .withColumn('ARST_CREATION_DATE', last_day('TRANS_DATE'))						
res_2_amount_DF = res_2.groupBy(group_by_list).agg(sum('SECURITY_AMOUNT').alias('AMOUNT'))
res_3_amount_DF = res_3.groupBy(group_by_list).agg(sum('LIABILITY_AMOUNT_DEBT').alias('AMOUNT'))
res_2_amount_DF.filter(col('TRANS_NUM') == 'D2023004CA-0102-000').show()
res_2.filter(col('TRANS_NUM') == 'D2023004CA-0102-000').show()
# res_2.filter(col('TRANS_NUM') == 'D2023003CA-0102-000').show()
#Code for expiry interest, it will have , res_4
res_4 = am_dealer_loc_DF.join(gd_loyalty_trans_DF,((gd_loyalty_trans_DF.TRANS_PARENT_GROUP == am_dealer_loc_DF.PARENT_GROUP)\
							& (gd_loyalty_trans_DF.TRANS_DLR_MAP_CD == am_dealer_loc_DF.DEALER_MAP_CD)\
							& (gd_loyalty_trans_DF.TRANS_LOC_CD == am_dealer_loc_DF.LOC_CD)\
							), 'inner')\
						.where((col('MUL_DEALER_CD') != '9967' ))
						
res_4 = res_4.join(gd_loyalty_enrol_DF,(gd_loyalty_enrol_DF.CARD_NUM == res_4.CARD_NUM), 'inner')

res_4 = res_4.join(am_list_range_DF,(am_list_range_DF.PRINCIPAL_MAP_CD == res_4.PRINCIPAL_MAP_CD)\
							& (gd_loyalty_enrol_DF.CARD_TYPE == am_list_range_DF.LIST_CODE)\
							& (gd_loyalty_enrol_DF.CHANNEL == am_list_range_DF.LIST_GRP_CODE)\
							, 'inner')\
			 .where(((col('LIST_FLAG').isNull()) | (col('LIST_FLAG') == 'Y')) \
			   & (col('LIST_NAME') == 'CARD_TYPE') ) 

		   
res_4 = res_4.select(
                lit('MUL AUTOCARD').alias('SOURCE') ,
                col('MUL_DEALER_CD').alias('MUL_DEALER_CD') ,
                col('FOR_CD').alias('FOR_CD') ,
                col('OUTLET_CD').alias('OUTLET_CD') ,
                col('DEALER_CATEGORY').alias('DEALER_CATEGORY') ,                
                lit(None).alias('ARST_BILL_TO_DEALER_CODE') ,
                lit(None).alias('ARST_BILL_TO_FOR_CODE') ,
                lit(None).alias('ARST_BILL_TO_OUTLET_CODE') ,
                lit(None).alias('ARST_CUSTOMER_CATEGORY') ,
                lit(None).alias('TRANS_NUM') ,
                col('EXPIRY_DATE').alias('TRANS_DATE'),
                lit(date.today()).alias('CURRENT_DATE') ,
                lit('INR').alias('ARST_CURRENCY_CODE') ,
                lit('USER').alias('ARST_CONVERSION_TYPE') ,
                lit('1').alias('ARST_CONVERSION_RATE') ,
                col('SECURITY_AMOUNT_BALANCE') ,
                lit(None).alias('ARST_REMARKS') ,
                lit(None).alias('ELEMENT') ,
                lit('S').alias('ARST_ACCOUNT_TYPE') ,
                lit('N').alias('ARST_FUND_VALIDATION') ,
                lit(None).alias('ARST_BATCH_NUMBER') ,
                lit('LINE').alias('LINE_TYPE') ,
                lit('1').alias('LINE_NUMBER') ,
                lit(None).alias('ARST_LINK_LINE') ,
                lit('R').alias('ARST_PROCESS_FLAG') ,
                lit('MISC').alias('ARST_PMOD_CODE') ,
                col('LIST_CODE').alias('CARD_TYPE') ,
                am_dealer_loc_DF.CHANNEL.alias('CHANNEL') ,
                lit(None).alias('CREATION_MONTH') ,
                lit(None).alias('FIN_YEAR') ,
                lit(None).alias('ARST_CREATION_DATE') ,
                lit(None).alias('TRANSFER_FLAG') ,
                col('PARENT_GROUP').alias('PARENT_GROUP') ,
                col('REGION_CD').alias('REGION_CD'))
res_4 = res_4.join(for_cd_DF, (( res_4.MUL_DEALER_CD == for_cd_DF.MUL_DEALER_CD1 )\
                             & ( res_4.FOR_CD == for_cd_DF.FOR_CD1  )\
                             & ( res_4.OUTLET_CD == for_cd_DF.OUTLET_CD1 )), 'left')
res_4 = res_4.select(col('SOURCE') ,
                    col('MUL_DEALER_CD') ,
                    col('FOR_CD') ,
                    col('ARST_BILL_TO_DEALER_CODE') ,
                    col('ARST_BILL_TO_FOR_CODE') ,
                    col('ARST_BILL_TO_OUTLET_CODE') ,
                    col('ARST_CUSTOMER_CATEGORY') ,
                    col('TRANS_NUM') ,
                    col('TRANS_DATE') ,
                    col('CURRENT_DATE'),
                    col('ARST_CURRENCY_CODE') ,
                    col('ARST_CONVERSION_TYPE') ,
                    col('ARST_CONVERSION_RATE') ,
                    col('SECURITY_AMOUNT_BALANCE') ,
                    col('ARST_REMARKS') ,
                    col('ELEMENT') ,
                    col('ARST_ACCOUNT_TYPE') ,
                    col('ARST_FUND_VALIDATION') ,
                    col('ARST_BATCH_NUMBER') ,
                    col('LINE_TYPE') ,
                    col('LINE_NUMBER') ,
                    col('ARST_LINK_LINE') ,
                    col('ARST_PROCESS_FLAG') ,
                    col('ARST_PMOD_CODE') ,
                    col('CARD_TYPE') ,
                    col('CHANNEL') ,
                    col('CREATION_MONTH') ,
                    col('FIN_YEAR') ,
                    col('ARST_CREATION_DATE') ,
                    col('TRANSFER_FLAG') ,
                    col('PARENT_GROUP') ,
                    col('REGION_CD') ,
                    col('LV_PARENT_GROUP') ,
                    col('LV_FOR_CD') ,
                    col('LV_DLR_CD') ,
                    col('LV_OUTLET_CD') 
)

res_4 = res_4.withColumn('ARST_BILL_TO_DEALER_CODE',col('LV_DLR_CD') )\
            .withColumn('ARST_BILL_TO_FOR_CODE',col('LV_FOR_CD') )\
            .withColumn('ARST_BILL_TO_OUTLET_CODE',col('LV_OUTLET_CD') )\
            .withColumn('ARST_CUSTOMER_CATEGORY',substring(col('LV_DLR_CD'),1,3) )
res_4 = res_4.withColumn('ELEMENT',when(substring(col('LV_DLR_CD'),1,3) == 'VCF', 'AC_RRPD').otherwise('AC_RRPSD') )

res_4 = res_4.withColumn('FINANCIAL_YEAR',when(month('TRANS_DATE') < 4, year('TRANS_DATE')-1).otherwise(year('TRANS_DATE')))\
            .withColumn('FINANCIAL_MONTH',when(month('TRANS_DATE') < 4, concat(lit('0'),month('TRANS_DATE') + 9)).otherwise(concat(lit('00'),month('TRANS_DATE') - 3)))\
            .withColumn('LV_BATCH_NUMBER',concat(lit('PBT'),col('FINANCIAL_YEAR'),col('FINANCIAL_MONTH')))

res_4 = res_4.withColumn('TRANS_NUM',when(col('CHANNEL') == 'COM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'PBT', 'P'),col('CARD_TYPE'),lit('C-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'EXC' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'PBT', 'P'),col('CARD_TYPE'),lit('N-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE')))\
                        .when(col('CHANNEL') == 'NRM' , concat(regexp_replace(col('LV_BATCH_NUMBER'),'PBT', 'P'),col('CARD_TYPE'),lit('A-'),substring(col('ARST_BILL_TO_DEALER_CODE'),4,20),lit('-'),col('ARST_BILL_TO_OUTLET_CODE'))))

res_4 = res_4.withColumn('ARST_REMARKS', when(col('CHANNEL') == 'COM' ,concat(lit('PRAGATI CREDIT AGAINST POINTS EXPIRED IN '), date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR')) )\
                        .otherwise(concat(lit('MARUTI SUZUKI REWARDS CREDIT AGAINST POINTS EXPIRED IN '), date_format('TRANS_DATE', 'MMM'),lit(' '), col('FINANCIAL_YEAR'))))

res_4 = res_4.withColumn('CREATION_MONTH', date_format('TRANS_DATE', 'MMM'))\
             .withColumn('FIN_YEAR', col('FINANCIAL_YEAR'))\
             .withColumn('ARST_BATCH_NUMBER', col('LV_BATCH_NUMBER'))\
             .withColumn('ARST_CREATION_DATE', last_day('TRANS_DATE'))	
# res_4.filter(col('TRANS_NUM') == 'P2023004CA-4704-01').show()
res_4_amount_DF = res_4.groupBy(group_by_list).agg(sum('SECURITY_AMOUNT_BALANCE').alias('AMOUNT'))

output_col_list = ['SOURCE', 'ARST_BILL_TO_DEALER_CODE', 'ARST_BILL_TO_FOR_CODE', 'ARST_BILL_TO_OUTLET_CODE', 'ARST_CUSTOMER_CATEGORY', 'TRANS_NUM',
                   'ARST_CURRENCY_CODE', 'ARST_CONVERSION_TYPE', 'ARST_CONVERSION_RATE', 'AMOUNT', 'ARST_REMARKS', 'ELEMENT', 'ARST_ACCOUNT_TYPE',
                   'ARST_FUND_VALIDATION', 'ARST_BATCH_NUMBER', 'LINE_TYPE', 'LINE_NUMBER', 'ARST_LINK_LINE', 'ARST_PROCESS_FLAG', 'ARST_PMOD_CODE',
                   'CARD_TYPE', 'CHANNEL', 'CREATION_MONTH', 'FIN_YEAR', 'ARST_CREATION_DATE', 'TRANSFER_FLAG', 'PARENT_GROUP', 'REGION_CD']

res_4_amount_DF = res_4_amount_DF.select(*output_col_list)
res_3_amount_DF = res_3_amount_DF.select(*output_col_list)
res_2_amount_DF = res_2_amount_DF.select(*output_col_list)
res_1_amount_DF = res_1_amount_DF.select(*output_col_list)

final_df = res_1_amount_DF.union(res_2_amount_DF)
final_df = final_df.union(res_3_amount_DF)
final_df = final_df.union(res_4_amount_DF)

small_df = final_df.filter((col('FIN_YEAR') == '2023') & (col('CREATION_MONTH') == 'Jul'))

small_df = small_df.withColumn('TRANSFER_FLAG',col("TRANSFER_FLAG").cast(StringType())) \
                    .withColumn('ARST_LINK_LINE',col("ARST_LINK_LINE").cast(StringType()))
small_df.coalesce(1)\
        .write.options(header='True', delimiter=',') \
        .mode('overwrite')\
        .csv(f"s3://msil-data-lake-raw/external/loyality_dashboard/")

print('Completed')