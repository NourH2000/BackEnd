/*************************  This route get the the data of the last training *************************/

/********* Overview => Overview PPA  *********/

const express = require("express");
const db = require("../../database");
const router = express.Router();

// test request
router.get("/", (req, res) => {
    res.json({ toto: "Details Of Training Route" });
  });
  const cassandra = require("cassandra-driver");
  const { request } = require("express");
  
  const client = new cassandra.Client({
    contactPoints: ["127.0.0.1"],
    localDataCenter: "datacenter1",
    keyspace: "frauddetection",
  });


  // get all the id of last training ( max )
router.get("/MaxTrainingP", (req, res) => {
    const query = "SELECT max(id_entrainement) as MaxId FROM ppa_result  ALLOW FILTERING   ;";
  
    client
      .execute(query, [], { prepare: true })
      .then((result) => {
        var MaxIdPpa = result;
        res.status(200).send(MaxIdPpa?.rows);
      })
      .catch((err) => {
        console.log("ERROR :", err);
      });
  });
  


module.exports = router;

