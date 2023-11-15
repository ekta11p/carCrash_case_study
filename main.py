from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import yaml
from utilities import utils
import argparse



class VehicleAccident:
    
    def __init__(self, config_file_path):
        """ read all input csv files using config.yaml file
        """
        
        input_file_path=utils.read_yaml(config_file_path).get('INPUT_FILE_PATH')
        self.charges=utils.read_csv_file(spark,input_file_path.get('Charges'))
        self.damages=utils.read_csv_file(spark,input_file_path.get('Damages'))
        self.endorse=utils.read_csv_file(spark,input_file_path.get('Endorse'))
        self.pri_person=utils.read_csv_file(spark,input_file_path.get('Primary_Person'))
        self.restrict=utils.read_csv_file(spark,input_file_path.get('Restrict'))
        self.units=utils.read_csv_file(spark,input_file_path.get('Units'))
        
    
    def no_of_crashes_male_killed(self, output_path,output_format):
        """ 
        Analysis 1: Finds the number of crashes in which killed person is Male.
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : count of dataframe
    
        """
        df=self.pri_person.filter(col('PRSN_INJRY_SEV_ID')=='KILLED'). \
            filter(col('PRSN_GNDR_ID')=='MALE')
        utils.write_output(df,output_path,output_format)
        return df.count()
    
    def two_wheeler_crashed(self,output_path,output_format):
        """ 
        Analysis 2: Finds the number of two wheelers booked for crash
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : count of dataframe
    
        """
        df=self.units.filter(col('VEH_BODY_STYL_ID').like('%MOTORCYCLE%'))
        utils.write_output(df,output_path,output_format)
        return df.count()
    
    def state_with_highest_no_of_accident_female(self,output_path,output_format):
        """ 
        Analysis 3: Finds the number of two wheelers booked for crash
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : name of state with highest accident
    
        """
        df=self.pri_person.filter(col('PRSN_GNDR_ID')=='FEMALE'). \
            groupby(col('DRVR_LIC_STATE_ID')).count().orderBy(col("count").desc()).drop('count')
        
        utils.write_output(df,output_path,output_format)
        
        return df.first().DRVR_LIC_STATE_ID
                                                              
    
    def vehicle_make_contribute_to_largest_injuries(self,output_path,output_format):
        """
        Analysis 4: Shows Top 5th to 15th vehicle maker that contribute to injuries including death
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : dataframe
        
        """
        w=Window.orderBy(col('TOTAL_INJURY_DEATH_AGG').desc())
        df=self.units.filter(col('VEH_MAKE_ID')!='NA'). \
                withColumn('TOTAL_INJURY_DEATH',col('TOT_INJRY_CNT')+col('DEATH_CNT')). \
                groupby('VEH_MAKE_ID').sum('TOTAL_INJURY_DEATH'). \
                withColumnRenamed('sum(TOTAL_INJURY_DEATH)','TOTAL_INJURY_DEATH_AGG'). \
                orderBy(col('TOTAL_INJURY_DEATH_AGG').desc()). \
                withColumn('RANK',row_number().over(w)). \
                filter((col('RANK')>=5) & (col('Rank')<=15)). \
                select("VEH_MAKE_ID", "TOTAL_INJURY_DEATH_AGG")
        
        utils.write_output(df,output_path,output_format)
        return df.show()
    
    def top_ethnic_user_group_crash(self,output_path,output_format):
        """
        Analysis 5: Finds top ethnic user group of each unique body style that was involved in crashes
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : dataframe
        """
        w=Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col("count").desc())
        df=self.units.join(self.pri_person ,on=['CRASH_ID'],how='inner'). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn('TOP',row_number().over(w)).filter(col('TOP')==1).drop('count','TOP')
                                                              
        utils.write_output(df,output_path,output_format) 
        
        return df.show()
                                                              
    def top_zip_codes_with_crashes_by_alcohol(self,output_path,output_format):
        """
        Analysis 6: Finds the top zip codes with highest number of crashes contributed by alcohol
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : list of zip codes
        
        """
        df=self.units.join(self.pri_person ,on=['CRASH_ID'],how='inner'). \
                dropna(subset=["DRVR_ZIP"]). \
                filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
                groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        
        utils.write_output(df,output_path,output_format) 
        
        return [row['DRVR_ZIP'] for row in df.collect()]
                                                              
    def crashes_with_no_damage_and_car_insurance(self,output_path,output_format):
        """
        Analysis 7: Number of distinct crash IDs with no damaged property , damage greater than level 4 and avails insurance
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : count of crash IDs
        
        """
        df=self.damages.join(self.units, on=["CRASH_ID"], how='inner') \
                  .filter(((self.units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & 
                           (~self.units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) 
                          
                          | ((self.units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                             (~self.units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))) \
                  .filter(self.damages.DAMAGED_PROPERTY == "NONE") \
                  .filter(col("FIN_RESP_TYPE_ID").contains("PROOF OF LIABILITY INSURANCE")) \
                  .select('CRASH_ID').distinct()
        
        utils.write_output(df,output_path,output_format) 
        return df.count()
                                                              
                                                              
    def get_top_5_vehicle_brand(self, output_path, output_format):
        """
        Analysis 8: Top 5 vehicle makes where drivers are charged with speeding and has licensed driver,
        uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences 
        :parameter output_path : path for output file
        :parameter output_format : output file format
        :return : List of Vehicle brands
        
        """
        top_10_vehicle_colors=[row['VEH_COLOR_ID'] for row in self.units.filter(self.units.VEH_COLOR_ID != "NA"). \
                            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        top_25_states=[row['VEH_LIC_STATE_ID'] for row in self.units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()). \
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
                                                              
                                                              
                                                              
        df=self.pri_person.join(self.charges,on=['CRASH_ID'],how='inner') \
                    .join(self.units,on=['CRASH_ID'],how='inner') \
                    .filter(self.charges.CHARGE.contains('SPEED')) \
                    .filter(self.pri_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])) \
                    .filter(self.units.VEH_COLOR_ID.isin(top_10_vehicle_colors)) \
                    .filter(self.units.VEH_LIC_STATE_ID.isin(top_25_states)) \
                    .groupby("VEH_MAKE_ID").count() \
                    .orderBy(col("count").desc()).limit(5) 
                                                              
        utils.write_output(df,output_path,output_format) 
        return [row[0] for row in df.collect()]


if __name__ == '__main__':
    
    """arguments
    :argument 1 : list of analysis number . eg. --analysis_numbers 1,2,3,...
    :argument 2 : output file format. eg. Output_csv , Output_parquet
    """
    parser = argparse.ArgumentParser(description='Vehicle Accident Analysis')
    parser.add_argument('--analysis_numbers', help='Enter the analysis number to perform (1-8)')
    parser.add_argument('--output_format', help='Output file format (e.g., Output_csv, Output_parquet)')
    
    
    args = parser.parse_args()
    
    #initialize spark
    spark=SparkSession.builder\
        .master('local')\
        .appName('VehicleAccident')\
        .getOrCreate()
    
    config_file_path = 'config.yaml'
    spark.sparkContext.setLogLevel("ERROR")
    
    #class with all analysis function
    veh_acc=VehicleAccident(config_file_path)
    output_file_paths = utils.read_yaml(config_file_path).get("OUTPUT_FILE_PATH")
    file_format = utils.read_yaml(config_file_path).get("FILE_FORMAT")
    
    output_format = args.output_format
    
    #User choice analyses selection
    
    selected_analyses = []
    if args.analysis_numbers:
        if args.analysis_numbers.lower() == 'all':
            selected_analyses = list(range(1, 9))
        else:
            selected_analyses = [int(num.strip()) for num in args.analysis_numbers.split(',')]
    
    if not selected_analyses:
        print("No valid analysis selected. Exiting.")
        spark.stop()
    
    else:
        for analysis_choice in selected_analyses:


            if analysis_choice == 1:
                print("Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?")
                result =veh_acc.no_of_crashes_male_killed(output_file_paths.get('Analysis_1'), file_format.get(output_format))
                print("Result:", result)

            elif analysis_choice == 2:
                print("Analysis 2: How many two-wheelers are booked for crashes?")
                result = veh_acc.two_wheeler_crashed(output_file_paths.get('Analysis_2'), file_format.get(output_format))
                print("Result:", result)

            elif analysis_choice == 3:
                print("Analysis 3: Which state has highest number of accidents in which females are involved? ")
                result=veh_acc.state_with_highest_no_of_accident_female(output_file_paths.get('Analysis_3'), file_format.get(output_format))
                print("Result:", result)


            elif analysis_choice == 4:
                print("Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death ")
                result=veh_acc.vehicle_make_contribute_to_largest_injuries(output_file_paths.get('Analysis_4'), file_format.get("Output_csv"))
                print("Result:", result)


            elif analysis_choice == 5:
                print("Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style")
                result=veh_acc.top_ethnic_user_group_crash(output_file_paths.get('Analysis_5'), file_format.get("Output_csv"))
                print("Result:", result)


            elif analysis_choice == 6:
                print("Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)")
                result=veh_acc.top_zip_codes_with_crashes_by_alcohol(output_file_paths.get('Analysis_6'), file_format.get("Output_csv"))
                print("Result:", result)


            elif analysis_choice == 7:
                print("Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")
                result=veh_acc.crashes_with_no_damage_and_car_insurance(output_file_paths.get('Analysis_7'), file_format.get("Output_csv"))
                print("Result:", result)


            elif analysis_choice == 8:
                print("Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)")
                result=veh_acc.get_top_5_vehicle_brand(output_file_paths.get('Analysis_8'), file_format.get("Output_csv"))
                print("Result:", result)


            else:
                print("Invalid analysis number. Enter a number from 1 to 8.")


    spark.stop()
          


