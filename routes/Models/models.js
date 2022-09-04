const pythonShellScript = require("../../helpers").pythonShellScript;

const { json } = require("body-parser");
const express = require("express");
const db = require("../../database");
const router = express.Router();

//data base connection
const cassandra = require("cassandra-driver");
const { request } = require("express");
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// test route
router.get("/", (req, res) => {
  res.json({ toto: "models" });
});

//get the max , min of  dates : 

// max , min of ppa source 

router.get("/getMinMaxdateP", (req, res) => {
  const query = "SELECT MIN(date_paiment)  as date_paiment_min , MAX(date_paiment) as date_paiment_max from  ppa_source;";
  try {
    client.execute(query, function (err, result) {
      var maxDate = result?.rows[0].date_paiment_max;
      var minDate = result?.rows[0].date_paiment_min;
      res.status(200).send({maxDate , minDate});
    });
  } catch (err) {
    console.log(err);
  }
});
// max , min of ppa source TMP

 router.get("/getMaxdatePTMP", (req, res) => {
  const query = "SELECT MIN(date_paiment)  as date_paiment_min , MAX(date_paiment) as date_paiment_max FROM ppa_source_TMP;";
  try {
    client.execute(query, function (err, result) {
      var maxDate = result?.rows[0].date_paiment_max;
      var minDate = result?.rows[0].date_paiment_min;
      res.status(200).send({maxDate , minDate});
    });
  } catch (err) {
    console.log(err);
  }
});


  // Quantity : 
// max , min of qyantity source 
router.get("/getMINMaxdateQ", (req, res) => {
    const query = "SELECT MIN(date_paiment)  as date_paiment_min , MAX(date_paiment) as date_paiment_max FROM quantity_source";
    try {
      client.execute(query, function (err, result) {
        var maxDate = result?.rows[0].date_paiment_max;
        var minDate = result?.rows[0].date_paiment_min;
        res.status(200).send({maxDate,minDate});
      });
    } catch (err) {
      console.log(err);
    }
  });

// max , min of quantity source TMP

router.get("/getMaxMindateQTMP", (req, res) => {
  const query = "SELECT MIN(date_paiment)  as date_paiment_min , MAX(date_paiment) as date_paiment_max FROM quantity_source_TMP";
  try {
    client.execute(query, function (err, result) {
      var maxDate = result?.rows[0].date_paiment_max;
      var minDate = result?.rows[0].date_paiment_min;
      //The row is an Object with column names as property keys.
      res.status(200).send({minDate , maxDate});
    });
  } catch (err) {
    console.log(err);
  }
});




  //call a model ( quantitymodel)
router.post("/QuantityTraining", (req, res) => {


  var options = {
    //scriptPath: '',
    //replace this dates with the ones you will receive from req.body
    args: [],
  };
  const path = "IAModels/QuantityEntraitement.py";
  try {
    pythonShellScript(path, options);
    console.log("hello am a quantity model ");
  } catch (err) {
    res.send(err);
  }
});


//call traitement ( quantitymodel)
router.post("/QuantityTraitement", (req, res) => {
  const date_debut = req.body.date_debut;
  const date_fin = req.body.date_fin;
  const auto = req.body.auto

  var options = {
    //scriptPath: '',
    //replace this dates with the ones you will receive from req.body
    args: [date_debut, date_fin , auto],
  };
  const path = "IAModels/QuantityTraitement.py";
  try {
    pythonShellScript(path, options);
    console.log("hello am a quantity Teatement ");
  } catch (err) {
    res.send(err);
  }
});




////*********/////

//call a model ( PPaModel)
router.post("/PrixppaTraitement", (req, res) => {
  const date_debut = req.body.date_debut;
  const date_fin = req.body.date_fin;
  

  const auto = req.body.auto;
  var options = {
    //scriptPath: '',
    //replace this dates with the ones you will receive from req.body
    args: [date_debut, date_fin , auto],
  };
  const path = "IAModels/PrixppaTraitement.py";
  try {
    pythonShellScript(path, options);
    console.log("hello am a ppa model ");
  } catch (err) {
    res.send(err);
  }
});




// get if the this training exists before
router.get("/TestTraining", (req, res) => {
  const query =
    "SELECT count(*) FROM history_treatement where type = ? and date_debut = ? and date_fin = ? and status = ? ALLOW FILTERING  ;";
  const type = req.query.type;
  const date_debut = req.query.date_debut;
  const date_fin = req.query.date_fin;
  const status = 1;
  client
    .execute(query, [type, date_debut, date_fin, status], { prepare: true })
    .then((result) => {
      var TestDate = result;
      res.status(200).send(TestDate?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

module.exports = router;
