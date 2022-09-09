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


// log in 

router.post('/login', (req, res) => {
   //const { username, password } = req.query
   const { username, password } = req.body
  
  
    if (username && password) {
      
        const query = "select * from users where username=? ALLOW FILTERING; ";
        client
    .execute(query, [username], { prepare: true })
    .then((result) => {
      
      if(result.rowLength > 0){

        bcrypt.compare(password ,result.rows[0].password , (err , response)=>{
          if(response){

            res.status(200).send({ msg: "You are connected" , connected:1 });
          }else{

            res.status(200).send({ msg: "Wrong pwd/usr" ,  connected:0 });
          }
        })

      }else{
        
        res.status(200).send({ msg: "user does not exist" ,  connected:-1 });

      }})}})




/* if(result.rowLength > 0){
        bcrypt.compare(password , result[0].password , (err , response)=>{
          if(response){
            res.status(200).send({ msg: "You are connected" });
            console.log("YES");
          }else{
            res.status(200).send({ msg: "Wrong pwd/usr" });
            console.log("Wrong pwd/usr");
          }
        })
        
        

      }else{
        res.status(200).send({ msg: "User does not exist" });
        console.log("User does not exist");

      }
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });*/
/*
if(result.length > 0){
        console.log("you are looged in ")
        res.status(200).send({ msg: "find user successfully" });
      
      }else{
        console.log("Wrong username/password")
        res.status(200).send({ msg: "user not find " });
      }*/

// test request
router.get("/", (req, res) => {
    res.json({ toto: "users" });
  });

module.exports = router;
