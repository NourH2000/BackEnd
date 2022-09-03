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
    spark = SparkSession.builder.appName('Quantity model ').getOrCreate()

    # connection to cassandra database and cnas keyspace
    cluster = Cluster(['127.0.0.1'])

    # get the Training_date
    today = date.today()

    # connection to cassandra database and fraud keyspace
    session = cluster.connect('frauddetection')

    # get the last id of training
    bigIdValue = session.execute("select * from params where param='Max_Id_Entrainement_Quantity';")
    id_training = bigIdValue.one().value

    # insert the History into cassandra table
    type_training = 1
    status = 0
    query = "INSERT INTO history_training (id , date , status , type ,accurency , f1 , precision , recall  ) VALUES (%s, %s ,%s ,%s,%s ,%s ,%s ,%s) "
    addToHistory = session.execute(
    query, [id_training, today, status, type_training, 0, 0 , 0 ,0])

    # Increment the id of training
    session.execute("UPDATE params SET value = value + 1 WHERE param ='Max_Id_Entrainement_Quantity' ;")
    # Get all data of training 
    query = "SELECT id , affection , age , gender , num_enr , ts ,quantite_med ,  decision   FROM quantity_training  LIMIT 100  ALLOW FILTERING ;"
    rows = session.execute(query)
    new_pd = pd.DataFrame(list(rows))

    ## the most 5 frequent value of medication 
    ## a supprimer
    #n = 10
    #table_med  = dftable['num_enr'].value_counts().index.tolist()[:n]
    #table_med

    #filter the n freqquent medication
    #new_pd = pd.DataFrame(dftable.loc[dftable["num_enr"].isin(table_med)])
    #new_pd.head(10)
    #new_pd = dftable
    #Transformations
####### Deal with NULL values

    ## 1/ id : drop rows with NULL id's
    new_pd = new_pd[new_pd['id'].notna()]


    ## 5/ num_enr : drop the num_enr NULL 
    new_pd = new_pd[new_pd['num_enr'].notna()]

    ## 6/ quantite_med : drop the quantite_med NULL 
    new_pd = new_pd[new_pd['quantite_med'].notna()]

    ## 6/ decision : drop the quantite_med NULL 
    new_pd = new_pd[new_pd['decision'].notna()]

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
    new_pd = new_pd.astype({"id":"double" ,"num_enr":"str" ,"quantite_med":"double" ,
                            "ts":"int" , "age":"int" , "gender":"int" , "decision":"int" })


    #new_pd = new_pd.astype({"id":"double", "num_enr":"str" ,"quantite_med":"double" ,
                            #"ts":"str" , "age":"int"    , "gender":"str" })
        
        

    ####### Deal with NULL values ( the NULL Values after casting )

    ## 1/ id : drop all rows ( except date_paiment : baceause i keept the NULL values )
    new_pd = new_pd.dropna(subset=["id",  "num_enr" , "quantite_med"  , "age" , "ts" , "gender","decision"  ], how='all')
    #new_pd = new_pd.dropna(subset=["id"  , "num_enr"  , "age" , "ts" , "gender" , "affection" , "quantite_med" ], how='all')
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

    sparkdf_exploded = sparkdf.withColumn("affection_exploded", F.explode(sparkdf.affection))

    ### Pivot the affections ( rows => columns )
    firstTable = sparkdf_exploded.groupBy("id").pivot("affection_exploded").count().na.fill(value=0)

    ### Get the list of available " affections "
    affection_list = firstTable.columns
    affection_list.remove('id')

    ### empty the od list anf Insert the new  list of affections ( type = 2 )
    # empty
    requied_aff = session.execute("TRUNCATE TABLE List_required")

    type_affection = '2'
    query = "INSERT INTO list_required (list , type) VALUES (%s, %s) "

    for affection in affection_list : 
        addToList = session.execute(
        query, [affection, type_affection])

    ## une table qui a toute les colonne ( sauvgrade )
    secondTable = sparkdf.withColumnRenamed("id", "id2")


    ## join la  table qui a toute les colonne ( sauvgrade ) avec la table des affections pivoté 
    sparkJoined = secondTable.join(firstTable, secondTable.id2 == firstTable.id, "inner").drop('id2')

    ## une table qui a toute les colonne ( sauvgrade )
    sparkJoinedSaved = sparkJoined.withColumnRenamed("id", "id3")

    # pivoter les medication en mettant 0 et 1 ( lignes => colonne )
    med = sparkJoined.groupBy("id").pivot("num_enr").count().na.fill(value=0)

    # ### Get the list of available " medications  "
    medication_list = med.columns
    medication_list.remove('id')

###Insert the new  list of medication ( type = 1 )

    # insert
    type_medication = '1'
    query = "INSERT INTO list_required (list , type) VALUES (%s, %s) "

    for medication in medication_list : 
        addToList = session.execute(
        query, [medication, type_medication])

    ## join la  table qui a toute les colonne ( sauvgrade ) avec la table des medication pivoté 

    df_final = sparkJoinedSaved.join(med, sparkJoinedSaved.id3 == med.id, "inner").drop('id3')

### garder les champs requise pour l'assembleur

    features = df_final.columns
    list_to_remove = ['quantite_med', 'ts', 'gender','age','decision','no_assure', 'fk', 'date_paiment','codeps','region','affection','num_enr','id']
    #removed str columns
    features = list(set(features) - set(list_to_remove))
    # sort the lefted columns (int) :
    features.sort(key=float)
    #reinsert the other requied other ( just the first items)
    for fea in list_to_remove[0:4] : 
        features.insert(0 , fea)
    

    ## cast the data 
    df_final = df_final \
    .withColumn("age" , df_final["age"].cast("integer")) \
    .withColumn("ts", df_final["ts"].cast("integer")) \
    .withColumn("gender", df_final["gender"].cast("integer")\

            )
### call the vector assembler
    from pyspark.ml.feature import VectorAssembler
    assembler =VectorAssembler(inputCols=features, outputCol="indexedFeatures")
    output = assembler.setHandleInvalid("keep").transform(df_final)


### choose the final dataframe 
    data = output.select("indexedFeatures", "decision")

    ### Split the data 
    (trainingData, testData ) = data.randomSplit([0.7 , 0.3])

    ### Call the model 
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator

    rf = RandomForestClassifier(featuresCol="indexedFeatures" , labelCol="decision" , numTrees=19 , maxBins=19)

    pipeline = Pipeline(stages=[rf])
### fit the model 
    model = pipeline.fit(trainingData)

### make the predictions with test data 
    predictions = model.transform(testData)

### evaluate the model 

#### Accurency and test error

    rfevaluator = MulticlassClassificationEvaluator(labelCol="decision", predictionCol="prediction", metricName="accuracy")
    accuracy = rfevaluator.evaluate(predictions)

#### F1
    rfevaluator = MulticlassClassificationEvaluator(labelCol="decision", predictionCol="prediction", metricName="f1")
    f1 = rfevaluator.evaluate(predictions) 
    
#### Precision

    rfevaluator = MulticlassClassificationEvaluator(labelCol="decision", predictionCol="prediction", metricName="precisionByLabel")
    precision = rfevaluator.evaluate(predictions)

#### Recall 

    rfevaluator = MulticlassClassificationEvaluator(labelCol="decision", predictionCol="prediction", metricName="recallByLabel")
    recall = rfevaluator.evaluate(predictions)

    ### Save the model 

    basePath = "/home/nourguealia/Documents/PFE/M"
    model.write().overwrite().save(basePath)

# insert into notification
   # Message
    msg = f'L\'entrainement {id_training} a été complété avec succès'
    ## 3 => entrainement quantity , 1 => traitement quantity , 2 => traitement PPA
    typeTraining = 3
    seen = 0
    status = 0
    queryNotification = "INSERT INTO notification (id  , id_entrainement , msg , seen , status , type ) VALUES (now() ,%s, %s ,%s , %s ,%s)"
    Notification = session.execute(
    queryNotification, [id_training, msg, seen, status,  typeTraining])



### add those result to the history training 
    success = 1
    query_success = "UPDATE history_training SET accurency ={}, f1 ={}, precision ={}, recall ={}, status ={}   WHERE id ={} and type = 1 ;".format(
        accuracy, f1 , precision,recall,  success, id_training)
    session.execute(query_success)







except Exception as e:
   print("An exception occurred")
   print(e)
   # set the status of the training = -1 ( failed)
   faild = -1

   query_success = "UPDATE history_training SET status ={} WHERE id ={} and type = 1 ;".format(
   faild, id_training)
   session.execute(query_success)

      # Message
   msg = f'L\'entrainement {id_training} a échoué'
   typeTraining = 3
   seen = 0
   status = 0

   Notification = session.execute(
   queryNotification, [id_training, msg, seen, status,  typeTraining])

