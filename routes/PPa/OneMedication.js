/******************  This route get the data of one medication in one training *******************/

/********* ALL TRAINING => ONE TRAINING BY MEDICATION => ONE DELEDICATION DEATILS *********/

const express = require("express");
const db = require("../../database");
const router = express.Router();

// test
router.get("/", (req, res) => {
  res.json({ msg: "am the Details of one training" });
});

const cassandra = require("cassandra-driver");
const { request } = require("express");

const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// get Details of one medication in the training  with NumEnR and idEntrainement
router.get("/OneMedication/", (req, res) => {
  const NumEnR = req.query.NumEnR;
  const idEntrainement = req.query.idEntrainement;
  const query =
    "SELECT * FROM ppa_result WHERE  num_enr= ? AND id_entrainement= ?  ALLOW FILTERING ";
  client
    .execute(query, [NumEnR, idEntrainement], { prepare: true })
    .then((result) => {
      var oneHistoryByNum_EnrAndIdEntrainement = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(oneHistoryByNum_EnrAndIdEntrainement?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// get count of each region One medication
router.get("/CountCenterSuspected/", (req, res) => {
  const query =
    "select count(*) as count , num_enr from ppa_result where id_entrainement =? and region =? and num_enr =? group by num_enr ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const numEnr = req.query.numEnr;
  const region = req.query.region;
  client
    .execute(query, [idEntrainement, region, numEnr], { prepare: true })
    .then((result) => {
      var ResultCountPerPharmacy = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerPharmacy?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});

// get count of each region where center and num_enr without group By num_enr
router.get("/CountOneCenterMedication/", (req, res) => {
  const query =
    "select *   from ppa_result where id_entrainement =? and region = ?  and num_enr=?    ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const region = req.query.region;
  const NumEnR = req.query.NumEnR;

  client
    .execute(query, [idEntrainement, region, NumEnR], { prepare: true })
    .then((result) => {
      console.log(result);
      var ResultCountPerPharmacy = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerPharmacy?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});

// count the number of medication with Codeps ( pharmacie) : this query will be traited in the front end , because we
// don't have a table that allows us to do a group by codeps query

// get count of each codeps ( codeps => num_enr )
router.get("/CountCodepsOneMedication/", (req, res) => {
  const query =
    "select codeps from ppa_result where id_entrainement =? and num_enr=?    ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const numEnr = req.query.numEnr;

  client
    .execute(query, [idEntrainement, numEnr], { prepare: true })
    .then((result) => {
      var ResultCountPerPharmacy = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerPharmacy?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});

// get count of all this medication
router.get("/CountMedication/", (req, res) => {
  const query =
    "select count_medicament_suspected as count from ppa_result where id_entrainement =? and num_enr=? LIMIT 1 ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const numEnr = req.query.numEnr;

  client
    .execute(query, [idEntrainement, numEnr], { prepare: true })
    .then((result) => {
      var ResultCountPerPharmacy = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerPharmacy?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});

// get count of each type ( outside )

router.get("/CountTypeOneMedication/", (req, res) => {
  const query =
    "select count(*) from ppa_result where id_entrainement =? and num_enr=?  and outside =?    ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const numEnr = req.query.numEnr;
  const outside = req.query.outside;

  client
    .execute(query, [idEntrainement, numEnr, outside], { prepare: true })
    .then((result) => {
      var ResultCountPerPharmacy = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerPharmacy?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});

// get count of each region ( region  )
router.get("/CountCenterMedication/", (req, res) => {
  const query =
    "select  region from ppa_result where id_entrainement =? and num_enr=?   ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const numEnr = req.query.numEnr;

  client
    .execute(query, [idEntrainement, numEnr], { prepare: true })
    .then((result) => {
      console.log(result);
      var ResultCountPerPharmacy = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerPharmacy?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});
module.exports = router;
