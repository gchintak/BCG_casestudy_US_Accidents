import logging
import time
import config as cfg
from pyspark.sql import SparkSession
from src.Analysis import Functions

logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.appName('BCG_CaseStudy').getOrCreate()

if __name__ == "__main__":

    func = Functions()
    op_path=cfg.output_path

    print('1st result: ', func.male_death(op_path['output_1']))
    print('2nd result: ', func.two_wheelers(op_path['output_2']))
    print('3rd result: ', func.top_5_makers(op_path['output_3']))
    print('4th result: ', func.valid_license_hit_run(op_path['output_4']))
    print('5th result: ', func.state_female_accidents())
    print('6th result: ', func.get_top_veh(op_path['output_6']))
    print('7th result: ', func.body_style(op_path['output_7']))
    print('8th result: ', func.top_zip_code(op_path['output_8']))
    print('9th result: ', func.no_damage_insurance(op_path['output_9']))
    print('10th result:', func.top_5_veh_color(op_path['output_10']))

    time.sleep(30)
    spark.stop()
