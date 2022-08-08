try:
    #!/usr/bin/env python
    # coding: utf-8

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

    # connection to cassandra database and fraud keyspace
    session = cluster.connect('frauddetection')

    # get the last id of training
    bigIdValue = session.execute(
        "select * from params where param='Max_Id_Entrainement_Ppa' ALLOW FILTERING ;")
    id_training = bigIdValue.one().value

    # insert the History into cassandra table
    type_training = 2
    status = 0

    query = "INSERT INTO History (id , date , status , type, date_debut , date_fin) VALUES (%s, %s ,%s ,%s,%s ,%s) "
    addToHistory = session.execute(
        query, [id_training, today, status, type_training, date_debut, date_fin])

    # Increment the id of training
    session.execute(
        "UPDATE params SET value = value + 1 WHERE param ='Max_Id_Entrainement_PPA' ;")

    query = "SELECT *  FROM ppa_source  WHERE date_paiment >= '{}' AND date_paiment <= '{}'  ALLOW FILTERING;".format(
        date_debut, date_fin)
    rows = session.execute(query)

    # transform the cassandra.cluster ( rows) to pandas dataframe to make some changes
    dftable = pd.DataFrame(list(rows))
    #print(dftable)

    # transformation :
    dftable = dftable.astype({"fk": float})
    dftable = dftable.astype({"date_paiment": str})

    # garder les coloumns qu'on est besoin
    dftable = dftable[['id', 'fk', 'codeps', 'date_paiment',
                       'num_enr', 'ts', 'prix_ppa', 'tier_payant']]

    # add column 'count_medicament' that gives the count of every num_enr
    dftable['count_Medicament'] = dftable.groupby(
        'num_enr')['num_enr'].transform('count')

    # add column 'count_medicament' that gives the count of every num_enr
    dftable['count_Medicament'] = dftable.groupby(
        'num_enr')['num_enr'].transform('count')

    bdprixPPA = spark.createDataFrame(dftable)

    # calculer q1 pour chaque medicament
    q1 = bdprixPPA.groupBy("num_enr").agg(
        f.percentile_approx("prix_ppa", 0.25).alias("q1"))

    # calculer q3 pour chaque medicament
    q3 = bdprixPPA.groupBy("num_enr").agg(
        f.percentile_approx("prix_ppa", 0.75).alias("q3"))

    # renommer la colonne
    bdprixPPA = bdprixPPA.withColumnRenamed("num_enr", "num_enr1")
    # faire une jointure entre la table et le q1 associé au medicament de la table
    bdprixPPA = bdprixPPA.join(q1, bdprixPPA.num_enr1 == q1.num_enr, "left")

    # supprimer la colonne pour ne pas avoir deux colonnes identique
    bdprixPPA = bdprixPPA.drop("num_enr")

    # faire une jointure entre la table et le q3 associé au medicament de la table
    bdprixPPA = bdprixPPA.join(q3, bdprixPPA.num_enr1 == q3.num_enr, "left")

    # supprimer la colonne pour ne pas avoir deux colonnes identique
    bdprixPPA = bdprixPPA.drop('num_enr1')

    # calcluer pour chaque medicament le prix minimum
    bdprixPPA = bdprixPPA.withColumn(
        "minPrix", (bdprixPPA['q1'] - (bdprixPPA['q3'] - bdprixPPA['q1'])*1.5))

    # calcluer pour chaque medicament le prix maximum
    bdprixPPA = bdprixPPA.withColumn(
        "maxPrix", (bdprixPPA['q3'] + (bdprixPPA['q3'] - bdprixPPA['q1'])*1.5))

    # creer une colonne outside : si le prix est entre [minPrix, maxPrix] prix est normal , sinon il est superieur ou inferieur a la normal
    bdprixPPA = bdprixPPA.withColumn("outside", when(
        (bdprixPPA["prix_ppa"] < bdprixPPA["minPrix"]), '-1').otherwise('0'))
    bdprixPPA = bdprixPPA.withColumn("outside", when(
        (bdprixPPA["prix_ppa"] > bdprixPPA["maxPrix"]), '1').otherwise(bdprixPPA['outside']))

    #bdprixPPA.show()

    # keep the suspected line
    Final_result = bdprixPPA.where("outside <> 0 ")

    # add Count_medicament_suspected that count the count of suspected time this drug shows
    from pyspark.sql import Window
    Final_result = Final_result.withColumn('Count_medicament_suspected', F.count(
        'num_enr').over(Window.partitionBy('num_enr')))

    # add Count_medicament_suspected_inf and sup that count the count of suspected inf of normal and sup
    from pyspark.sql import Window
    Final_result = Final_result.withColumn('count_medicament_inf', F.count(
        when(col("outside") == -1, True)).over(Window.partitionBy('num_enr')))
    Final_result = Final_result.withColumn('count_medicament_sup', F.count(
        when(col("outside") == 1, True)).over(Window.partitionBy('num_enr')))

    # count the codeps
    Final_result = Final_result.withColumn(
        'count_pharmacy', F.count('codeps').over(Window.partitionBy('codeps')))

    # iterate the data (insert it into cassandra keyspace) :
    data_collect = Final_result.collect()
    #Final_result.show()

    query = "INSERT INTO ppa_result (id , id_entrainement , num_enr , count_medicament_suspected, region , codeps , count_medicament , count_medicament_inf , count_medicament_sup , date_debut , date_fin , date_entrainement  , fk  , prix_ppa , prix_min , prix_max , outside , ts ,  count_pharmacy ,date_paiment , tier_payant  ) VALUES (now() ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s  ,%s ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s  )"
    queryPha = "INSERT INTO Pharmacy_result (id , id_entrainement , num_enr ,codeps , count_pharmacy , region , date_debut , date_fin , date_entrainement , fk , prix_ppa , prix_min , prix_max , outside , ts , tier_payant , date_paiment   ) VALUES (now() ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s  ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s  )"
    queryNotification = "INSERT INTO notification (id  , id_entrainement , msg , seen , status , type ) VALUES (now() ,%s, %s ,%s , %s ,%s)"

    for row in data_collect:
        future = session.execute(query, [id_training, row["num_enr"], row["Count_medicament_suspected"], row["region"], row["codeps"], row["count_Medicament"], row["count_medicament_inf"], row["count_medicament_sup"],
                                 date_debut, date_fin, today, row["fk"], row["prix_ppa"], row["minPrix"], row["maxPrix"], row["outside"],  row["ts"],  row["count_pharmacy"], row["date_paiment"], row["tier_payant"]])

        futurePhar = session.execute(queryPha, [id_training, row["num_enr"], row["codeps"], row["count_pharmacy"],  row["region"], date_debut,
                                     date_fin, today, row["fk"], row["prix_ppa"], row["minPrix"], row["maxPrix"], row["outside"], row["ts"], row["tier_payant"], ["date_paiment"]])

    # set the status of the training = 1 ( success)
    success = 1

    query_success = "UPDATE History SET status ={}  WHERE id ={} and type = 2 ;".format(
        success, id_training)
    session.execute(query_success)

    # insert into notification
    # Message
    msg = f'The PPA training number {id_training} was completed successfully'
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

    query_success = "UPDATE History SET status ={} WHERE id ={} and type = 2 ;".format(
        faild, id_training)
    session.execute(query_success)

    # insert into notification
    # Message
    msg = f'The training number {id_training} was completed successfully'
    typeTraining = 1
    seen = 0
    status = 0

    Notification = session.execute(
        queryNotification, [id_training, msg, seen, status,  typeTraining])
