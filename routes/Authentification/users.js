const express = require("express");
const db = require("../../database");
const router = express.Router();

const cassandra = require("cassandra-driver");
const { request } = require("express");

const bcrypt = require('bcrypt')
const saltRounds = 10



const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// test request
router.get("/", (req, res) => {
  res.json({ toto: "users" });
});

// get all the users ( don't forget to define the feilds of the users table )
router.get("/AllUsers", async (req, res) => {
  console.log(req.user);

  const query = "SELECT * FROM users";
  try {
    client.execute(query, function (err, result) {
      var users = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(users?.rows);
    });
  } catch (err) {
    console.log(err);
  }
});

// add one user
router.post("/AddUser", (req, res) => {
  console.log("ho")
  const { username, password } = req.query
  if (username && password) {

    bcrypt.hash(password , saltRounds , (err, hash)=>{ 
    const query = "INSERT INTO users (username, password) VALUES(?, ?) ";
      
    client
      .execute(query, [username, hash])
      .then((result) => {
        console.log(result);

        res.status(200).send({ msg: "created user successfully" });
        console.log("created user successfully ");
      })
      .catch((err) => {
        console.log("ERROR :", err);
      });

    })

  
  }
});

module.exports = router;
