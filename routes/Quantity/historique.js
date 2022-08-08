/*************************  This route get the history of all training *************************/

/********* ALL TRAINING  *********/

const express = require("express");
const db = require("../../database");
const router = express.Router();

// test request
router.get("/", (req, res) => {
  res.json({ toto: "historiqueRoute" });
});
const cassandra = require("cassandra-driver");
const { request } = require("express");

const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// get all the History Of Training
router.get("/ByTraining", (req, res) => {
  const query = "SELECT * FROM History where type = ? ALLOW FILTERING  ;";
  const type = 1;

  client
    .execute(query, [type], { prepare: true })
    .then((result) => {
      var historyOfTraining = result;
      res.status(200).send(historyOfTraining?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

module.exports = router;
