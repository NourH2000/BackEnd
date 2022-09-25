const express = require("express");
const db = require("../../database");
const router = express.Router();

const cassandra = require("cassandra-driver");
const { request } = require("express");

const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// test request
router.get("/", (req, res) => {
  res.json({ toto: "historiqueRoute" });
});

// get count of notifications unseen yet
router.get("/NotificationCount", (req, res) => {
  const query =
    "SELECT  count(*) FROM notification where seen = ?   ALLOW FILTERING   ;";
  const seen = 0;

  client
    .execute(query, [seen], { prepare: true })
    .then((result) => {
      var historyOfTraining = result;
      res.status(200).send(historyOfTraining?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// get data of notifications unseen yet
router.get("/NotificationData", (req, res) => {
  const query =
    "SELECT * FROM notification where seen = ?  ALLOW FILTERING   ;";
  const seen = 0;

  client
    .execute(query, [seen], { prepare: true })
    .then((result) => {
      var UnseenNotifications = result;
      res.status(200).send(UnseenNotifications?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// get data of notifications unseen yet
router.get("/NotificationAllId", (req, res) => {
  const query = "select id from notification where seen = ? ALLOW FILTERING;";
  const seen = 0;
  client
    .execute(query, [seen], { prepare: true })
    .then((result) => {
      var idsOfUnseenNotification = result;
      res.status(200).send(idsOfUnseenNotification?.rows);
    })
    .catch((err) => {
      console.log("ER0OR :", err);
    });
});

// update the status of notifications ==> unseen
router.get("/NotificationUpdate", (req, res) => {
  const query = "UPDATE notification SET seen =1  where id=? ;";
  
  const ids = (req.query.ids);
  client
    .execute(query, [ids], { prepare: true })
    .then((result) => {
      var updatedNotification = result;
      res.status(200).send(updatedNotification?.rows);
    })
    .catch((err) => {
      console.log("ER0OR :", err);
     
    });
});


// get all notifications 



router.get("/AllNotification", (req, res) => {
  const query = "select msg , seen , status from notification ALLOW FILTERING;";

  client
    .execute(query)
    .then((result) => {
      var updatedNotification = result;
      res.status(200).send(updatedNotification?.rows);
    })
    .catch((err) => {
      console.log("ER0OR :", err);
     
    });
});


module.exports = router;
