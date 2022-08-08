const express = require("express");
const db = require("../../database");
const router = express.Router();

// database connection
const cassandra = require("cassandra-driver");
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "user",
});

// test request
router.get("/", (req, res) => {
  res.json({ toto: "users" });
});

// get all the users ( don't forget to define the feilds of the users table )
router.get("/AllUsers", async (req, res) => {
  console.log(req.user);

  const query = "SELECT * FROM user";
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
  const { username, password } = req.body;
  if (username && password) {
    const query = "INSERT INTO user (username, pwd) VALUES(?, ?) ";
    client
      .execute(query, [username, password])
      .then((result) => {
        console.log(result);

        res.status(200).send({ msg: "created user successfully" });
      })
      .catch((err) => {
        console.log("ERROR :", err);
      });
  }
});

module.exports = router;
