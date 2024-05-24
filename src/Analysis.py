import config as cfg
from pyspark.sql.functions import col, row_number, regexp_extract, when
from pyspark.sql import Window, Row
from src.utils import  load_csv_data_to_df, save_csv

class Functions:
    def __init__(self):
      input_file = cfg.input_file_name
      self.charges = load_csv_data_to_df(input_file['Charges'])
      self.damages = load_csv_data_to_df(input_file['Damages'])
      self.endorse = load_csv_data_to_df(input_file['Endorse'])
      self.primary_person = load_csv_data_to_df(input_file['Primary_Person'])
      self.units = load_csv_data_to_df(input_file['Units'])
      self.restrict = load_csv_data_to_df(input_file['Restrict'])
    
    def male_death(self,path):
        """
        Returns number of crashes (accidents) in which number of males killed are greater than 2
        """
        # filtering with male persons who are died
        death_df = self.primary_person.filter((self.primary_person['DEATH_CNT'] == 1) & (self.primary_person['PRSN_GNDR_ID'] == 'MALE'))
        counts = death_df.groupby('CRASH_ID').count()
        filtered_counts = counts.filter(counts['count'] > 2) # gives dataframe that has crash_id with more than 2 male death
        save_csv(filtered_counts,path)
        return filtered_counts.count()

    def two_wheelers(self,path):
        """
        returns count of two wheelers that are booked for crashes
        """
        df=self.units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")) #filters with motorcycle
        save_csv(df,path)
        return df.count()

    def top_5_makers(self,path):
        """
        Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy
        """
        units_person_df=self.units.select('CRASH_ID', 'VEH_MAKE_ID').join(self.primary_person, on='CRASH_ID', how='inner')
        #filtering with driver, airbags not deployed and died
        filtered_df = units_person_df.filter((col('PRSN_AIRBAG_ID')=="NOT DEPLOYED") &
                                             (col('PRSN_TYPE_ID')=='DRIVER') &
                                             (col('DEATH_CNT')==1) &
                                             (col('VEH_MAKE_ID')!='NA'))
        top_5=filtered_df.groupby('VEH_MAKE_ID').count().orderBy(col('count').desc()).limit(5).select('VEH_MAKE_ID')
        save_csv(top_5, path)
        return [row['VEH_MAKE_ID'] for row in top_5.collect()]
    
    def valid_license_hit_run(self,path):
        """
        Returns number of Vehicles with driver having valid licences involved in hit and run
        """
        df2 = self.units.select('CRASH_ID', 'VEH_HNR_FL').join(self.primary_person.select('CRASH_ID', 'DRVR_LIC_TYPE_ID'), on='CRASH_ID', how='inner')
        fil_df2 = df2.filter((col('VEH_HNR_FL') == 'Y') & 
                             (col('DRVR_LIC_TYPE_ID').isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])))
        save_csv(fil_df2, path)
        return fil_df2.count()
    
    def state_female_accidents(self):
        """
        Returns state that has highest number of accidents in which females are not involved
        """
        df = self.primary_person.filter((col('PRSN_GNDR_ID') != 'FEMALE') & (~col('DRVR_LIC_STATE_ID').isin(['NA', 'Unknown'])))
        state_counts = df.groupby('DRVR_LIC_STATE_ID').count()
        max_state = state_counts.orderBy(col('count').desc()).first()[0]
        return max_state
    
    def get_top_veh(self,path):
        """
        Returns Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = self.units.filter(col('VEH_MAKE_ID') != 'NA')
        agg_df = df.groupby('VEH_MAKE_ID').agg({'TOT_INJRY_CNT': 'sum', 'DEATH_CNT': 'sum'})
        agg_df = agg_df.withColumn('TOTAL_INJRY_DEATH', agg_df['sum(TOT_INJRY_CNT)'] + agg_df['sum(DEATH_CNT)'])
        sorted_vehicles = agg_df.orderBy('TOTAL_INJRY_DEATH', ascending=False)
        top_vehicles = sorted_vehicles.limit(5)
        top_vehicles_list= top_vehicles.select('VEH_MAKE_ID')
        save_csv(top_vehicles_list,path)
        return [row['VEH_MAKE_ID'] for row in top_vehicles_list.collect()[2:5]]

    def body_style(self,path):
        """
        returns top ethnic user group of each unique body style  
        """
        df = self.units.select('CRASH_ID', 'VEH_BODY_STYL_ID').join(self.primary_person.select('CRASH_ID', 'PRSN_ETHNICITY_ID'),
                                                                           on='CRASH_ID', how='inner')
        fil_df = df.filter((~col('VEH_BODY_STYL_ID').isin(['NA', 'UNKNOWN', 'NOT REPORTED', 'OTHER  (EXPLAIN IN NARRATIVE)'])) & 
                        (~col('PRSN_ETHNICITY_ID').isin(['NA', 'UNKNOWN'])))
        
        grouped = fil_df.groupby(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']).count().withColumnRenamed('count', 'counts')
        window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('counts').desc())
        ranked_df = grouped.withColumn('rank', row_number().over(window_spec)).filter(col('rank') == 1).drop('rank')

        res_df =ranked_df.select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
        save_csv(res_df,path)
        res_df.show()
        return res_df
    
    def top_zip_code(self,path):
        """
        Returns Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash
        """
        df = self.primary_person.filter((col('PRSN_ALC_RSLT_ID') == 'Positive') & (col('DRVR_ZIP').isNotNull()))
        zip_counts = df.groupby('DRVR_ZIP').count().orderBy(col('count').desc()).limit(5)
        zip_list= zip_counts.select('DRVR_ZIP')
        save_csv(zip_list,path)
        return [row['DRVR_ZIP'] for row in zip_list.collect()]

    def no_damage_insurance(self,path):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance
        """
        df = self.damages.join(self.units, on='CRASH_ID', how='inner')
        fil_df = df.filter((df['FIN_RESP_TYPE_ID'] == 'PROOF OF LIABILITY INSURANCE') &
                        (~df['VEH_DMAG_SCL_1_ID'].isin(['NA', 'NO DAMAGE'])) &
                        (~df['VEH_DMAG_SCL_2_ID'].isin(['NA', 'NO DAMAGE'])) &
                        (df['DAMAGED_PROPERTY'].isin(['NONE', 'NONE1'])))

        fil_df = fil_df.withColumn('VEH_DMAG_SCL_1_VAL', regexp_extract('VEH_DMAG_SCL_1_ID', r'\d+', 0).cast('int'))
        fil_df = fil_df.withColumn('VEH_DMAG_SCL_2_VAL', regexp_extract('VEH_DMAG_SCL_2_ID', r'\d+', 0).cast('int'))

        res_df=fil_df.filter((fil_df['VEH_DMAG_SCL_1_VAL'] > 4) | (fil_df['VEH_DMAG_SCL_2_VAL'] > 4)).select('CRASH_ID').distinct()
        save_csv(res_df,path)
        return res_df.count()

    def top_5_veh_color(self,path):
        """
        the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
        """
        top_10_colors = self.units.groupBy('VEH_COLOR_ID').count().orderBy(col('count').desc()).limit(10).select('VEH_COLOR_ID').collect()
        top_25_states = self.units.groupBy('VEH_LIC_STATE_ID').count().orderBy(col('count').desc()).limit(25).select('VEH_LIC_STATE_ID').collect()

        top_10_colors_list = [row['VEH_COLOR_ID'] for row in top_10_colors]
        top_25_states_list = [row['VEH_LIC_STATE_ID'] for row in top_25_states]
    
        mer_df1 = self.charges.join(self.primary_person, on='CRASH_ID', how='inner')
        mer_df2 = mer_df1.join(self.units, on='CRASH_ID', how='inner')
        mer_df2 = mer_df2.withColumn('CHARGE', when(col('CHARGE').isNull(), '').otherwise(col('CHARGE')))
        df = mer_df2.filter((col('CHARGE').contains('SPEED')) &
                            (col('DRVR_LIC_TYPE_ID').isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])))
        color_state_df = df.filter((col('VEH_COLOR_ID').isin(top_10_colors_list)) &
                                (col('VEH_LIC_STATE_ID').isin(top_25_states_list)))
        
        res_list = color_state_df.groupBy('VEH_MAKE_ID').count().orderBy(col('count').desc()).limit(5).select('VEH_MAKE_ID')
        save_csv(res_list,path)
        return [row['VEH_MAKE_ID'] for row in res_list.collect()]
