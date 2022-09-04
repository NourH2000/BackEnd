try:
    import pyspark
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as func
    import pyspark.sql.functions as f
    from pyspark.sql.functions import when
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
    from datetime import date


    

    # ouvrir une session spark
    spark = SparkSession.builder.appName('prixEx').getOrCreate()

    # connection to cassandra database and cnas keyspace
    cluster = Cluster(['127.0.0.1'])

    # get the Training_date
    today = date.today()


    # get parameters (start_date and end_date)
    date_debut = sys.argv[1]
    date_fin = sys.argv[2]
    auto = sys.argv[3]
  

    

    # connection to cassandra database and fraud keyspace
    session = cluster.connect('frauddetection')

    # get the last id of training
    bigIdValue = session.execute(
    "select * from params where param='Max_Id_Entrainement_Ppa' ALLOW FILTERING ;")
    id_training = bigIdValue.one().value
    print(id_training)
    # insert the History into cassandra table
    type_training = 2
    status = 0

    query = "INSERT INTO history_treatement (id , date , status , type, date_debut , date_fin) VALUES (%s, %s ,%s ,%s,%s ,%s) "
    addToHistory = session.execute(
        query, [id_training, today, status, type_training, date_debut, date_fin])


    session.execute(
    "UPDATE params SET value = value + 1 WHERE param ='Max_Id_Entrainement_Ppa';")

    ## if the treatment by dates or auto ( all and just new data )
    if (auto == 'Oui'):
        query = "SELECT *  FROM ppa_source_TMP   ALLOW FILTERING  ;"
        print("hello am an auto ")
    else :
        query = "SELECT *  FROM ppa_source  WHERE date_paiment >= '{}' AND date_paiment <= '{}' LIMIT 10  ALLOW FILTERING  ;".format(
        date_debut, date_fin)
        print("hello am not auto ")
    
    
    
    rows = session.execute(query)

    # transform the cassandra.cluster ( rows) to pandas dataframe to make some changes
    dftable = pd.DataFrame(list(rows))
    #dftable = dftable.astype({"fk": float})
    #dftable = dftable.astype({"date_paiment": str})


    # In[47]:


    ##visualisation
    #dftable.count()
    dftable.info()
    dftable.isna().sum()


    # In[48]:


    #Transformations
    ####### Deal with NULL values

    ## 1/ id : drop rows with NULL id's
    dftable = dftable[dftable['id'].notna()]

    ## 2/ date_paiment : keep value of date with NULL

    ## 3/ codeps : replace the codeps NULL with -1
    #dftable.codeps.fillna(value='-1', inplace=True)

    ## 4/ fk : replace the fk NULL with -1
    dftable.fk.fillna(value='-1', inplace=True)

    ## 5/ num_enr : drop the num_enr NULL 
    dftable = dftable[dftable['num_enr'].notna()]

    ## 6/ prix_ppa : drop the prix_ppa NULL 
    dftable = dftable[dftable['prix_ppa'].notna()]

    ## 7/ region : replace the region NULL with -1
    dftable.region.fillna(value='-1', inplace=True)

    ## 8/ tier_payant : replace the tier payant NULL with -1
    dftable.tier_payant.fillna(value='-1', inplace=True)

    ## 9/ ts : replace ts NULL with -1
    dftable.ts.fillna(value='-1', inplace=True)


    ####### Cast the columns to the right types 
    dftable = dftable.astype({"id":"double" , "codeps":"str" , "fk":"str" , "num_enr":"str" ,"prix_ppa":"double" ,
                            "region":"int" , "tier_payant":"str" , "ts":"str" , "date_paiment":"str" })

    ####### Deal with NULL values ( the NULL Values after casting )

    ## 1/ id : drop all rows ( except date_paiment : baceause i keept the NULL values )
    dftable = dftable.dropna(subset=["id", "codeps" ,"fk" , "num_enr" , "prix_ppa" , "region" , "tier_payant" , "ts" ], how='all')


    # In[49]:


    #dftable.info()
    #dftable.isna().sum()


    # In[50]:


    #add column 'count_medicament' that gives the count of every num_enr
    dftable['count_Medicament'] = dftable.groupby('num_enr')['num_enr'].transform('count')


    # In[51]:


    # add column 'count_medicament' that gives the count of every num_enr
    dftable['count_Medicament'] = dftable.groupby(
    'num_enr')['num_enr'].transform('count')


    # In[52]:


    bdprixPPA = spark.createDataFrame(dftable)


    # In[53]:


    # calculer q1 pour chaque medicament
    q1 = bdprixPPA.groupBy("num_enr").agg(
    f.percentile_approx("prix_ppa", 0.25).alias("q1"))


    # In[54]:


    # calculer q3 pour chaque medicament
    q3 = bdprixPPA.groupBy("num_enr").agg(
    f.percentile_approx("prix_ppa", 0.75).alias("q3"))


    # In[55]:


    # renommer la colonne
    bdprixPPA = bdprixPPA.withColumnRenamed("num_enr", "num_enr1")
    # faire une jointure entre la table et le q1 associé au medicament de la table
    bdprixPPA = bdprixPPA.join(q1, bdprixPPA.num_enr1 == q1.num_enr, "left")


    # In[56]:


    # supprimer la colonne pour ne pas avoir deux colonnes identique
    bdprixPPA = bdprixPPA.drop("num_enr")


    # In[57]:


    # faire une jointure entre la table et le q3 associé au medicament de la table
    bdprixPPA = bdprixPPA.join(q3, bdprixPPA.num_enr1 == q3.num_enr, "left")


    # In[58]:


    # supprimer la colonne pour ne pas avoir deux colonnes identique
    bdprixPPA = bdprixPPA.drop('num_enr1')


    # In[59]:


    # calcluer pour chaque medicament le prix minimum
    bdprixPPA = bdprixPPA.withColumn(
    "minPrix", (bdprixPPA['q1'] - (bdprixPPA['q3'] - bdprixPPA['q1'])*1.5))


    # In[60]:


    # calcluer pour chaque medicament le prix maximum
    bdprixPPA = bdprixPPA.withColumn(
    "maxPrix", (bdprixPPA['q3'] + (bdprixPPA['q3'] - bdprixPPA['q1'])*1.5))


    # In[61]:


    # creer une colonne outside : si le prix est entre [minPrix, maxPrix] prix est normal , sinon il est superieur ou inferieur a la normal
    bdprixPPA = bdprixPPA.withColumn("outside", when(
    (bdprixPPA["prix_ppa"] < bdprixPPA["minPrix"]), -1).otherwise('0'))
    bdprixPPA = bdprixPPA.withColumn("outside", when(
    (bdprixPPA["prix_ppa"] > bdprixPPA["maxPrix"]), 1).otherwise(bdprixPPA['outside']))


    # In[62]:


    # keep the suspected line
    Final_result = bdprixPPA.where("outside <> 0 ")


    # In[63]:


    # add Count_medicament_suspected that count the count of suspected time this drug shows
    from pyspark.sql import Window
    Final_result = Final_result.withColumn('Count_medicament_suspected', F.count(
    'num_enr').over(Window.partitionBy('num_enr')))


    # In[64]:


    # add Count_medicament_suspected_inf and sup that count the count of suspected inf of normal and sup
    from pyspark.sql import Window
    Final_result = Final_result.withColumn('count_medicament_inf', F.count(
    when(col("outside") == -1, True)).over(Window.partitionBy('num_enr')))
    Final_result = Final_result.withColumn('count_medicament_sup', F.count(
    when(col("outside") == 1, True)).over(Window.partitionBy('num_enr')))


    # In[65]:


    # count the codeps
    Final_result = Final_result.withColumn(
    'count_pharmacy', F.count('codeps').over(Window.partitionBy('codeps')))


    # In[87]:


    ## Transformation 
        ## Cast the columns to the right values 
    Final_result = Final_result.withColumn("id",Final_result["id"].cast("integer"))\
                            .withColumn("codeps",Final_result["codeps"].cast("string"))\
                                .withColumn("fk",Final_result["fk"].cast("string"))\
                                .withColumn("no_assure",Final_result["no_assure"].cast("string"))\
                                .withColumn("prix_ppa",Final_result["prix_ppa"].cast("double"))\
                                .withColumn("region",Final_result["region"].cast("int"))\
                                .withColumn("tier_payant",Final_result["tier_payant"].cast("string"))\
                                .withColumn("ts",Final_result["ts"].cast("string"))\
                                .withColumn("count_Medicament",Final_result["count_Medicament"].cast("int"))\
                                .withColumn("q1",Final_result["q1"].cast("double"))\
                                .withColumn("q3",Final_result["q3"].cast("double"))\
                                .withColumn("num_enr",Final_result["num_enr"].cast("string"))\
                                .withColumn("minPrix",Final_result["minPrix"].cast("double"))\
                                .withColumn("maxPrix",Final_result["maxPrix"].cast("double"))\
                                .withColumn("outside",Final_result["outside"].cast("int"))\
                                .withColumn("Count_medicament_suspected",Final_result["Count_medicament_suspected"].cast("int"))\
                                .withColumn("count_medicament_inf",Final_result["count_medicament_inf"].cast("int"))\
                                .withColumn("count_medicament_sup",Final_result["count_medicament_sup"].cast("int"))\
                                .withColumn("count_pharmacy",Final_result["count_pharmacy"].cast("int"))\



    # In[88]:


    data_collect = Final_result.collect()


    # In[115]:


    query = "INSERT INTO ppa_result (id , id_entrainement , num_enr , count_medicament_suspected , codeps , count_medicament , count_medicament_inf , count_medicament_sup , date_debut , date_fin , date_entrainement  , fk  , prix_ppa , prix_min , prix_max , outside , ts ,  count_pharmacy ,date_paiment , tier_payant , no_assure ,region , q1 , q3  ) VALUES (now() ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s  ,%s ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s  )"

    queryPha = "INSERT INTO ppa_pharmacy (id , id_entrainement , num_enr ,codeps , count_pharmacy , region , date_debut , date_fin , date_entrainement , fk , prix_ppa , prix_min , prix_max , outside , ts , tier_payant , date_paiment   ) VALUES (now() ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s  )"

    queryNotification = "INSERT INTO notification (id  , id_entrainement , msg , seen , status , type ) VALUES (now() ,%s, %s ,%s , %s ,%s)"


# In[114]:



    
    for row in data_collect:
        future = session.execute(query, [id_training, row["num_enr"], row["Count_medicament_suspected"], row["codeps"], row["count_Medicament"], row["count_medicament_inf"], row["count_medicament_sup"],
         date_debut, date_fin, today, row["fk"], row["prix_ppa"], row["minPrix"], row["maxPrix"], row["outside"],  row["ts"],  row["count_pharmacy"], row["date_paiment"], row["tier_payant"] ,row["no_assure"] ,row["region"] ,row["q1"] ,row["q3"]])

        futurePhar = session.execute(queryPha, [id_training, row["num_enr"], row["codeps"], row["count_pharmacy"],  row["region"], date_debut,
        date_fin, today, row["fk"], row["prix_ppa"], row["minPrix"], row["maxPrix"], row["outside"], row["ts"], row["tier_payant"], row["date_paiment"]])

    # set the status of the training = 1 ( success)
    success = 1

    query_success = "UPDATE history_treatement SET status ={}  WHERE id ={} and type = 2 ;".format(
        success, id_training)
    session.execute(query_success)

    # insert into notification
    # Message
    msg = f'The PPA training number {id_training} was completed successfully'
    ## 3 => entrainement quantity , 1 => traitement quantity , 2 => traitement PPA
    typeTraining = 2
    seen = 0
    status = 1

    Notification = session.execute(
        queryNotification, [id_training, msg, seen, status,  typeTraining])


except Exception as e:
    print("An exception occurred")
    print(e)
    # set the status of the training = -1 ( failed)
    faild = -1

    query_success = "UPDATE history_treatement SET status ={} WHERE id ={} and type = 2 ;".format(
        faild, id_training)
    session.execute(query_success)

    # insert into notification
    # Message
    msg = f'The training number {id_training} was completed successfully'
    typeTraining = 1
    seen = 0
    status = 0
    queryNotification = "INSERT INTO notification (id  , id_entrainement , msg , seen , status , type ) VALUES (now() ,%s, %s ,%s , %s ,%s)"

    Notification = session.execute(
        queryNotification, [id_training, msg, seen, status,  typeTraining])
