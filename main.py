from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

url = 'jdbc:postgresql://localhost:5432/spark'
table = 'json'
driver = 'org.postgresql.Driver'
user = 'postgres'
password = 'johanes'

spark = SparkSession.builder.appName('JSON').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# you can download json file from https://www.yelp.com/dataset/download
df = spark.read.format('json').option('inferSchema','true').load('D:/Python/Jupyter/yelp_dataset/yelp_academic_dataset_business.json')

ambience_schema = T.StructType(
    [
        T.StructField('casual',T.StringType(),True),
        T.StructField('classy',T.StringType(),True),
        T.StructField('divey',T.StringType(),True),
        T.StructField('hipster',T.StringType(),True),
        T.StructField('intimate',T.StringType(),True),
        T.StructField('romantic',T.StringType(),True),
        T.StructField('touristy',T.StringType(),True),
        T.StructField('trendy',T.StringType(),True),
        T.StructField('upscale',T.StringType(),True),
    ]
)

bp_schema = T.StructType(
    [
        T.StructField('garage', T.StringType(), True),
        T.StructField('lot', T.StringType(), True),
        T.StructField('street', T.StringType(), True),
        T.StructField('valet', T.StringType(), True),
        T.StructField('validated', T.StringType(), True),
    ]
)

gm_schema = T.StructType(
    [
        T.StructField('breakfast', T.StringType(), True),
        T.StructField('brunch', T.StringType(), True),
        T.StructField('dessert', T.StringType(), True),
        T.StructField('dinner', T.StringType(), True),
        T.StructField('latenight', T.StringType(), True),
        T.StructField('lunch', T.StringType(), True),
    ]
)

bn_schema = T.StructType(
    [
        T.StructField('mondaybestnights', T.StringType(), True),
        T.StructField('tuesdaybestnights', T.StringType(), True),
        T.StructField('wednesdaybestnights', T.StringType(), True),
        T.StructField('thursdaybestnights', T.StringType(), True),
        T.StructField('fridaybestnights', T.StringType(), True),
        T.StructField('saturdaybestnights', T.StringType(), True),
        T.StructField('sundaybestnights', T.StringType(), True),
    ]
)

df = (df.withColumn('acceptsinsurance', F.col('attributes.AcceptsInsurance'))
        .withColumn('agesallowed', F.col('attributes.AgesAllowed'))
        .withColumn('alcohol', F.col('attributes.Alcohol'))
        .withColumn('ambience', F.col('attributes.Ambience'))
        .withColumn('byob', F.col('attributes.BYOB'))
        .withColumn('byobcorkage', F.col('attributes.BYOBCorkage'))
        .withColumn('bestnights', F.col('attributes.BestNights'))
        .withColumn('bikeparking', F.col('attributes.BikeParking'))
        .withColumn('businessacceptsbitcoin', F.col('attributes.BusinessAcceptsBitcoin'))
        .withColumn('businessacceptscreditcards', F.col('attributes.BusinessAcceptsCreditCards'))
        .withColumn('businessparking', F.col('attributes.BusinessParking'))
        .withColumn('byappointmentonly', F.col('attributes.ByAppointmentOnly'))
        .withColumn('caters', F.col('attributes.Caters'))
        .withColumn('coatcheck', F.col('attributes.CoatCheck'))
        .withColumn('corkage', F.col('attributes.Corkage'))
        .withColumn('dietaryrestrictions', F.col('attributes.DietaryRestrictions'))
        .withColumn('dogsallowed', F.col('attributes.DogsAllowed'))
        .withColumn('drivethru', F.col('attributes.DriveThru'))
        .withColumn('goodfordancing', F.col('attributes.GoodForDancing'))
        .withColumn('goodforkids', F.col('attributes.GoodForKids'))
        .withColumn('goodformeal', F.col('attributes.GoodForMeal'))
        .withColumn('hairspecializesin', F.col('attributes.HairSpecializesIn'))
        .withColumn('happyhour', F.col('attributes.HappyHour'))
        .withColumn('hastv', F.col('attributes.HasTV'))
        .withColumn('music', F.col('attributes.Music'))
        .withColumn('noiselevel', F.col('attributes.NoiseLevel'))
        .withColumn('open24hours', F.col('attributes.Open24Hours'))
        .withColumn('outdoorseating', F.col('attributes.OutdoorSeating'))
        .withColumn('restaurantsattire', F.col('attributes.RestaurantsAttire'))
        .withColumn('restaurantscounterservice', F.col('attributes.RestaurantsCounterService'))
        .withColumn('restaurantsdelivery', F.col('attributes.RestaurantsDelivery'))
        .withColumn('restaurantsgoodforgroups', F.col('attributes.RestaurantsGoodForGroups'))
        .withColumn('restaurantspricerange2', F.col('attributes.RestaurantsPriceRange2'))
        .withColumn('restaurantsreservations', F.col('attributes.RestaurantsReservations'))
        .withColumn('restaurantstableservice', F.col('attributes.RestaurantsTableService'))
        .withColumn('restaurantstakeout', F.col('attributes.RestaurantsTakeOut'))
        .withColumn('smoking', F.col('attributes.Smoking'))
        .withColumn('wheelchairaccessible', F.col('attributes.WheelchairAccessible'))
        .withColumn('wifi', F.col('attributes.WiFi'))
        .withColumn('hourinfriday', F.col('hours.Friday'))
        .withColumn('hourinmonday', F.col('hours.Monday'))
        .withColumn('hourinsaturday', F.col('hours.Saturday'))
        .withColumn('hourinsunday', F.col('hours.Sunday'))
        .withColumn('hourinthursday', F.col('hours.Thursday'))
        .withColumn('hourintuesday', F.col('hours.Tuesday'))
        .withColumn('hourinwednesday', F.col('hours.Wednesday'))
        .withColumn('ambience', F.regexp_replace('ambience','False',"'False'"))
        .withColumn('ambience', F.regexp_replace('ambience','True',"'True'"))
        .withColumn('ambience', F.regexp_replace('ambience', "u'divey'","'divey'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "u'monday'", "'mondaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "u'tuesday'", "'tuesdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "u'wednesday'", "'wednesdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "u'thursday'", "'thursdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "u'friday'", "'fridaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "u'saturday'", "'saturdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights',"u'sunday'", "'sundaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "'monday'", "'mondaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "'tuesday'", "'tuesdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "'wednesday'", "'wednesdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "'thursday'", "'thursdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "'friday'", "'fridaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights', "'saturday'", "'saturdaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights',"'sunday'", "'sundaybestnights'"))
        .withColumn('bestnights', F.regexp_replace('bestnights',"False","'False'"))
        .withColumn('bestnights', F.regexp_replace('bestnights',"True","'True'"))
        .withColumn('alcohol', F.regexp_replace('alcohol', "u'none'", 'None'))
        .withColumn('alcohol', F.regexp_replace('alcohol', "'none'", 'None'))
        .withColumn('alcohol', F.regexp_replace('alcohol', "u'full_bar'", 'full bar'))
        .withColumn('alcohol', F.regexp_replace('alcohol', "'full_bar'", 'full bar'))
        .withColumn('alcohol', F.regexp_replace('alcohol', "'beer_and_wine'", 'beer and wine'))
        .withColumn('alcohol', F.regexp_replace('alcohol', "u'beer_and_wine'", 'beer and wine'))
        .withColumn('businessparking', F.regexp_replace('businessparking',"u'valet'","'valet'"))
        .withColumn('businessparking', F.regexp_replace('businessparking',"u'garage'","'garage'"))
        .withColumn('businessparking', F.regexp_replace('businessparking',"u'lot'","'lot'"))
        .withColumn('businessparking', F.regexp_replace('businessparking',"u'street'","'street'"))
        .withColumn('businessparking', F.regexp_replace('businessparking',"u'validated'","'validated'"))
        .withColumn('businessparking', F.regexp_replace('businessparking',"False","'False'"))
        .withColumn('businessparking', F.regexp_replace('businessparking',"True","'True'"))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "'average'", 'average'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "u'average'", 'average'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "uaverage", 'average'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "u'loud'", 'loud'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "'loud'", 'loud'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "u'quiet'", 'quiet'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "'quiet'", 'quiet'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "'very_loud'", 'very loud'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "u'very_loud'", 'very loud'))
        .withColumn('noiselevel', F.regexp_replace('noiselevel', "uvery loud", 'very loud'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "'casual'", 'casual'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "u'casual'", 'casual'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "'dressy'", 'dressy'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "u'formal'", 'formal'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "'formal'", 'formal'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "u'dressy'", 'dressy'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "ucasual", 'casual'))
        .withColumn('restaurantsattire', F.regexp_replace('restaurantsattire', "udressy", 'dressy'))
        .withColumn('wifi', F.regexp_replace('wifi', "'paid'", 'paid'))
        .withColumn('wifi', F.regexp_replace('wifi', "u'paid'", 'paid'))
        .withColumn('wifi', F.regexp_replace('wifi', "upaid", 'paid'))
        .withColumn('wifi', F.regexp_replace('wifi', "'no'", 'no'))
        .withColumn('wifi', F.regexp_replace('wifi', "u'no'", 'no'))
        .withColumn('wifi', F.regexp_replace('wifi', "uno", 'no'))
        .withColumn('wifi', F.regexp_replace('wifi', "'free'", 'free'))
        .withColumn('wifi', F.regexp_replace('wifi', "u'free'", 'free'))
        .withColumn('wifi', F.regexp_replace('wifi', "ufree", 'free'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal', "u'breakfast'", 'breakfast'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal', "u'brunch'", 'brunch'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal', "u'lunch'", 'lunch'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal', "u'dinner'", 'dinner'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal', "u'latenight'", 'latenight'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal', "u'dessert", 'dessert'))
        .withColumn('goodformeal', F.regexp_replace('goodformeal','False',"'False'"))
        .withColumn('goodformeal', F.regexp_replace('goodformeal','True',"'True'"))
        .withColumn('ambience', F.from_json(F.col('ambience'), ambience_schema))
        .withColumn('businessparking', F.from_json(F.col('businessparking'), bp_schema))
        .withColumn('goodformeal', F.from_json(F.col('goodformeal'), gm_schema))
        .withColumn('bestnights', F.from_json(F.col('bestnights'), bn_schema))
        .withColumn('casual', F.col('ambience.casual'))
        .withColumn('classy', F.col('ambience.classy'))
        .withColumn('divey', F.col('ambience.divey'))
        .withColumn('hipster', F.col('ambience.hipster'))
        .withColumn('intimate', F.col('ambience.intimate'))
        .withColumn('romantic', F.col('ambience.romantic'))
        .withColumn('touristy', F.col('ambience.touristy'))
        .withColumn('trendy', F.col('ambience.trendy'))
        .withColumn('upscale', F.col('ambience.upscale'))
        .withColumn('garage', F.col('businessparking.garage'))
        .withColumn('lot', F.col('businessparking.lot'))
        .withColumn('street', F.col('businessparking.street'))
        .withColumn('valet', F.col('businessparking.valet'))
        .withColumn('validated', F.col('businessparking.validated'))
        .withColumn('breakfast', F.col('goodformeal.breakfast'))
        .withColumn('brunch', F.col('goodformeal.brunch'))
        .withColumn('dessert', F.col('goodformeal.dessert'))
        .withColumn('dinner', F.col('goodformeal.dinner'))
        .withColumn('latenight', F.col('goodformeal.latenight'))
        .withColumn('lunch', F.col('goodformeal.lunch'))
        .withColumn('mondaybestnights', F.col('bestnights.mondaybestnights'))
        .withColumn('tuesdaybestnights', F.col('bestnights.tuesdaybestnights'))
        .withColumn('wednesdaybestnights', F.col('bestnights.wednesdaybestnights'))
        .withColumn('thursdaybestnights', F.col('bestnights.thursdaybestnights'))
        .withColumn('fridaybestnights', F.col('bestnights.fridaybestnights'))
        .withColumn('saturdaybestnights', F.col('bestnights.saturdaybestnights'))
        .withColumn('sundaybestnights', F.col('bestnights.sundaybestnights'))
        .drop('hours','attributes')
        .drop('ambience','businessparking','goodformeal','bestnights')
     )

df.write.format('jdbc').option('driver', driver).option('url', url).option('dbtable', table).option('user', user).option('password', password).save()