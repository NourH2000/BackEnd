try:

   from pyspark.sql import Window
   from pyspark.sql.functions import round, col
   from datetime import date
   from pyspark.ml.regression import LinearRegression
   from pyspark.ml.feature import VectorAssembler
   from pyspark.ml.feature import StringIndexer
   from pyspark.sql.functions import col, concat_ws
   from pyspark.sql.functions import udf
   import pyspark.sql.functions as F
   import sys
   from cassandra.cluster import Cluster
   import pandas as pd
   from pyspark.sql import SparkSession
   from pyspark import SparkConf, SparkContext
   from pyspark.sql import SQLContext
   import numpy as np
   from pyspark.sql.functions import split, col

   # new spark session
   spark = SparkSession.builder.appName('Quantité Traitement ').getOrCreate()

   # connection to cassandra database and cnas keyspace
   cluster = Cluster(['127.0.0.1'])

   # get the Training_date
   today = date.today()
   # get parameters (start_date and end_date)

   date_debut = sys.argv[1]
   date_fin = sys.argv[2]

   # connection to cassandra database and fraud keyspace
   session = cluster.connect('frauddetection')

   # get the last id of training
   bigIdValue = session.execute("select * from params where param='Max_Id_Tratement_Quantity';")
   id_treatement = bigIdValue.one().value

   # insert the History into cassandra table
   type_training = 1
   status = 0
   query = "INSERT INTO history_treatement (id , date , status , type, date_debut , date_fin) VALUES (%s, %s ,%s ,%s,%s ,%s) "
   addToHistory = session.execute(
   query, [id_treatement, today, status, type_training, date_debut, date_fin])

   # Increment the id of training
   session.execute("UPDATE params SET value = value + 1 WHERE param ='Max_Id_Tratement_Quantity' ;")

   query = "SELECT *  FROM quantity_source  WHERE date_paiment >= '{}' AND date_paiment <= '{}' LIMIT 3000 ALLOW FILTERING;".format(
   date_debut, date_fin)
   #query = "SELECT *  FROM quantity_source ALLOW FILTERING;"
   #query = "SELECT *  FROM quantity_trainingtmp where decision = 1 ALLOW FILTERING;"

   rows = session.execute(query)
   dftable = pd.DataFrame(list(rows))
   new_pd = dftable
   #Transformations
   ####### Deal with NULL values

   ## 1/ id : drop rows with NULL id's
   new_pd = new_pd[new_pd['id'].notna()]

   ## 2/ id : drop rows with NULL Date de paiement
   new_pd = new_pd[new_pd['date_paiment'].notna()]

   ## 2/ date_paiment : keep value of date with NULL

   ## 3/ codeps : replace the codeps NULL with -1
   new_pd.codeps.fillna(value='Agent_cnas', inplace=True)

   ## 4/ fk : replace the fk NULL with -1
   new_pd.fk.fillna(value='-1', inplace=True)

   ## 5/ num_enr : drop the num_enr NULL 
   new_pd = new_pd[new_pd['num_enr'].notna()]

   ## 6/ quantite_med : drop the quantite_med NULL 
   new_pd = new_pd[new_pd['quantite_med'].notna()]

   ## 6/ decision : drop the quantite_med NULL 
   #new_pd = new_pd[new_pd['decision'].notna()]

   ## 7/ region : replace the region NULL with -1
   new_pd.region.fillna(value='-1', inplace=True)

   ## 9/ ts : replace ts NULL with N
   new_pd.ts.fillna(value='N', inplace=True)


   ## 5/ age : drop the age NULL 
   new_pd = new_pd[new_pd['age'].notna()]

   ## 5/ gender : drop the gender NULL 
   new_pd = new_pd[new_pd['gender'].notna()]

   # remplacer None avec -1 ( aucune affection) dans la coloumn affection
   new_pd.affection.fillna(value='-1' ,inplace=True)
   #new_pd = new_pd .loc[new_pd["affection"] == '-1']

         

   ####### Cast the columns to the right types 
   new_pd = new_pd.astype({"id":"double" , "codeps":"str" , "fk":"str" , "num_enr":"str" ,"quantite_med":"double" ,
                           "ts":"int" , "age":"int"  , "date_paiment":"str"  , "gender":"int" , "region":"int"  })


   #new_pd = new_pd.astype({"id":"double", "num_enr":"str" ,"quantite_med":"double" ,
                           #"ts":"str" , "age":"int"    , "gender":"str" })
      
      

   ####### Deal with NULL values ( the NULL Values after casting )

   ## 1/ id : drop all rows ( except date_paiment : baceause i keept the NULL values )
   new_pd = new_pd.dropna(subset=["id", "codeps" ,"fk" , "num_enr" , "quantite_med" , "region" , "age" , "ts" , "gender","date_paiment" ], how='all')
   #new_pd = new_pd.dropna(subset=["id"  , "num_enr"  , "age" , "ts" , "gender" , "affection" , "quantite_med" ], how='all')

   # add column 'count_medicament' that gives the count of every num_enr
   new_pd['count_Medicament'] = dftable.groupby('num_enr')['num_enr'].transform('count')
   
   # creat a sparkDF
   sparkdf = spark.createDataFrame(new_pd)
   # transform the affection column to array of int ( splited by ',')
   sparkdf = sparkdf.withColumn("affection", split(
   col("affection"), ",").cast("array<int>"))

   # Sort the array of affections
   sparkdf = sparkdf.withColumn('affection', F.array_sort('affection'))

   #  age function 
   @udf("String")
   def age_range(age):
      if age >= 0 and age <= 5:
            return 0
      elif age > 5 and age <= 10:
            return 1
      elif age > 10 and age <= 16:
            return 2
      elif age > 16 and age <= 24:
            return 3
      elif age > 24 and age <= 60:
            return 4
      elif age > 60 and age <= 76:
            return 5
      else:
            return 6

   #coder l'age
   sparkdf = sparkdf.withColumn("age", age_range(col("age")))

   # exploder les affection 
   sparkdf_exploded = sparkdf.withColumn("affection_exploded", F.explode(sparkdf.affection))

   type_affection = 2
   affection_listQuery = "SELECT list from list_required  WHERE type = '{}'  ALLOW FILTERING;".format(
   type_affection)
   affection_listRows = session.execute(affection_listQuery)
   affection_list = spark.createDataFrame(list(affection_listRows))

# ##  keep just the rows that contain a requied affections 

##  keep just the rows that contain a requied affections 
   sparkdf_exploded.createOrReplaceTempView("exploded_affection_view")
   affection_list.createOrReplaceTempView("affection_list_view")

   affection_keeped = spark.sql("""
      select * 
      from exploded_affection_view E
      where affection_exploded IN (
               select list from affection_list_view A
               
         )
   """)
# ## get the missing affections ( and insert in into data base)

   affection_Not_keeped = spark.sql("""
      select * 
      from affection_list_view A
      where list NOT IN (
               select affection_exploded from exploded_affection_view E
               
         )
   """)
   # get the distinct affection and converrt em to a list 
   affection_Not_keeped = affection_Not_keeped.select('list').distinct()
   affection_Not_keeped_list=affection_Not_keeped.rdd.map(lambda x: x.list).collect()

   # convert it into str list ( to create a column with em ( name of column must be str ))
   # ## empty the old missing data and reinsert the new ones 

   # empty the table 
   missing = session.execute("TRUNCATE TABLE List_missing ;")
   # insert into the table 

   type_affection = 2
   query = "INSERT INTO List_missing (list , type) VALUES (%s, %s) "

   for affection in affection_Not_keeped_list : 
      addToList = session.execute(
      query, [str(affection), type_affection])


   # # Add the affections missing as a column with 0

   # ## Pivot the affections ( rows => columns )
   # pivoter les affection en mettant 0 et 1 ( lignes => colonne )
   firstTable = affection_keeped.groupBy("id").pivot("affection_exploded").count().na.fill(value=0)

   # ## add the missing affections as a column =0 
   from pyspark.sql.functions import lit
   for column in affection_Not_keeped_list : 
      firstTable = firstTable.withColumn(str(column), lit(0))

   ## une table qui a toute les colonne ( sauvgrade )
   secondTable = sparkdf.withColumnRenamed("id", "id2")

   ## join la  table qui a toute les colonne ( sauvgrade ) avec la table des affections pivoté 
   sparkJoined = secondTable.join(firstTable, secondTable.id2 == firstTable.id, "inner").drop('id2')

   # une table qui a toute les colonne ( sauvgrade )
   sparkJoinedSaved = sparkJoined.withColumnRenamed("id", "id3")

   # # keep just the medications in the required list 

   # ## get all the medications in the requied list

   type_med = 1
   med_listQuery = "SELECT list from list_required  WHERE type = '{}'  ALLOW FILTERING;".format(
   type_med)
   med_listRows = session.execute(med_listQuery)
   med_list = spark.createDataFrame(list(med_listRows))
   # ##  keep just the rows that contain a requied medications

   sparkJoinedSaved.createOrReplaceTempView("num_enr_view")
   med_list.createOrReplaceTempView("med_list_view")

   med_keeped = spark.sql("""
      select * 
      from num_enr_view N
      where num_enr IN (
               select list from med_list_view M
               
         )
   """)
# ## get the missing medications ( and insert in into data base)
   med_Not_keeped = spark.sql("""
      select * 
      from med_list_view M
      where list NOT IN (
               select num_enr from num_enr_view E
               
         )
   """)
   # get the distinct affection and converrt em to a list 
   med_Not_keeped = med_Not_keeped.select('list').distinct()
   med_Not_keeped_list=med_Not_keeped.rdd.map(lambda x: x.list).collect()

   # convert it into str list ( to create a column with em ( name of column must be str ))


   # ## empty the old missing data and reinsert the new ones 


   # empty the table 
   missing = session.execute("TRUNCATE TABLE List_missing ;")

   # insert into the table 
   type_med = 1
   query = "INSERT INTO List_missing (list , type) VALUES (%s, %s) "

   for med in med_Not_keeped_list : 
      addToList = session.execute(
      query, [str(med), type_med])


   # # Add the medications missing as a column with 0
   # 

   # ## Pivot the medications ( rows => columns )
   #  

   # pivoter les medication en mettant 0 et 1 ( lignes => colonne )

   med = med_keeped.groupBy("id3").pivot("num_enr").count().na.fill(value=0)
   #med.show()


   # ## add the missing medications as a column =0 

   from pyspark.sql.functions import lit
   for column in med_Not_keeped_list : 
      med = med.withColumn(str(column), lit(0))      

   # ## get list of available medications
   ## join la  table qui a toute les colonne ( sauvgrade ) avec la table des medication pivoté 
   med = med.withColumnRenamed("id3", "id")
   df_final = sparkJoinedSaved.join(med, sparkJoinedSaved.id3 == med.id, "inner").drop('id3')
   df_final.columns


   # ### garder les champs requise pour l'assembleur

   features = df_final.columns
   list_to_remove = ['quantite_med', 'ts', 'gender','age','no_assure', 'fk', 'date_paiment','codeps','region','affection','num_enr','id','count_Medicament','decision']
   #removed str columns
   features = list(set(features) - set(list_to_remove))
   # sort the lefted columns (int) :
   features.sort(key=float)
   #reinsert the other requied other ( just the first items)
   for fea in list_to_remove[0:4] : 
      features.insert(0 , fea)
   #features

   df_final = df_final \
   .withColumn("age" , df_final["age"].cast("integer")) \
   .withColumn("ts", df_final["ts"].cast("integer")) \
   .withColumn("gender", df_final["gender"].cast("integer")) 

   # ### call the vector assembler
   from pyspark.ml.feature import VectorAssembler
   assembler =VectorAssembler(inputCols=features, outputCol="indexedFeatures")
   output = assembler.setHandleInvalid("keep").transform(df_final)


   # ### choose the final dataframe 
   data = output.select("indexedFeatures")
   # ### Call the model 

   from pyspark.ml.classification import RandomForestClassifier
   from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
   from pyspark.ml import Pipeline , PipelineModel
   from pyspark.ml.evaluation import MulticlassClassificationEvaluator

   basePath = "/home/nourguealia/Documents/PFE/M"
   model = PipelineModel.load(basePath)

   # Make predictions.
   predictions = model.transform(output)
   # keep the suspected line
   predictions = predictions.where("prediction == 1 ")

   # add Count_medicament_suspected that count the count of suspected time this drug shows
   Final_result = predictions.withColumn('Count_medicament_suspected', F.count(
  'num_enr').over(Window.partitionBy('num_enr')))


   # ### Get count of each insured 

   # count the num_assuré
   Final_result = Final_result.withColumn('count_assure', F.count(
   'no_assure').over(Window.partitionBy('no_assure')))


   # ### transform age into categories
   # tranform the age 
   @udf("String")
   def age_range(age):
      if age == 0 :
            return '0-5'
      elif age == 1 :
            return '6-10'
      elif age == 2 :
            return '11-16'
      elif age == 3 :
            return '17-24'
      elif age == 4 :
            return '25-60'
      elif age == 5 :
            return '61-76'
      else:
            return '75+'

   Final_result = Final_result.withColumn("age", age_range(col("age")))


   ### Insert the result into database
   # iterate the data (insert it into cassandra keyspace) :
   data_collect = Final_result.collect()
   query = "INSERT INTO Quantity_result (id , fk ,  no_assure , id_entrainement , quantite_med   , count_medicament , count_medicament_suspected   , num_enr  , date_entrainement ,date_paiment ,affection , age , region , codeps , date_debut , date_fin , gender, ts , decision ) VALUES (now(), %s, %s, %s, %s, %s , %s, %s , %s ,%s , %s, %s ,%s , %s ,%s ,%s ,%s ,%s ,%s )"
   queryAssure = "INSERT INTO quantity_assure (id , fk , no_assure , id_entrainement , quantite_med , count_assure  , num_enr , date_entrainement ,date_paiment ,affection , age , region , codeps , date_debut , date_fin , gender , ts ,count_medicament_suspected , decision  ) VALUES (now(), %s ,%s ,%s ,%s ,%s,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)"
   queryNotification = "INSERT INTO notification (id  , id_entrainement , msg , seen , status , type ) VALUES (now() ,%s, %s ,%s , %s ,%s)"

   for row in data_collect:
      qnt = session.execute(query, [row["fk"] ,row["no_assure"], id_treatement ,row["quantite_med"] , row["count_Medicament"], row["Count_medicament_suspected"] , row["num_enr"] 
                                    , today , row["date_paiment"], str(row["affection"]) , row["age"] , row["region"] , row["codeps"] , date_debut, date_fin , row["gender"] , row["ts"] , int(row["prediction"]) ] )

      Assure = session.execute(queryAssure, [row["fk"], row["no_assure"], id_treatement, row["quantite_med"] ,row["count_assure"], row["num_enr"], today, row["date_paiment"], str(row["affection"]), row["age"], row["region"], row["codeps"], date_debut, date_fin, row["gender"], row["ts"], row["Count_medicament_suspected"] , int(row["prediction"])])


   success = 1

   query_success = "UPDATE history_treatement SET status ={}  WHERE id ={} and type = 1 ;".format(
   success, id_treatement)
   session.execute(query_success)

   # insert into notification
   # Message
   msg = f'Le traitement {id_treatement} a été complété avec succès'
   ## 3 => entrainement quantity , 1 => traitement quantity , 2 => traitement PPA
   typeTraining = 1
   seen = 0
   status = 0

   Notification = session.execute(
   queryNotification, [id_treatement, msg, seen, status,  typeTraining])

except Exception as e:
   print("An exception occurred")
   print(e)
   # set the status of the training = -1 ( failed)
   faild = -1

   query_success = "UPDATE history_treatement SET status ={} WHERE id ={} and type = 1 ;".format(
   faild, id_treatement)
   session.execute(query_success)

      # Message
   msg = f'Le traitement {id_treatement} a échoué'
   
   typeTraining = 1
   seen = 0
   status = 0

   Notification = session.execute(
   queryNotification, [id_treatement, msg, seen, status,  typeTraining])
