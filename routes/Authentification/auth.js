const express = require("express");
const db = require("../../database");
const router = express.Router();

const cassandra = require("cassandra-driver");
const { request } = require("express");
const bodyParser = require("body-parser")
//const expressSession = require("express-session")



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
            req.session.user = result
            console.log("You are connected")
            // Rahim Rahim
            res.status(200).send({ msg: "You are connected" , connected:1 });
          }else{
            console.log("Wrong pwd/usr")
            res.status(200).send({ msg: "Wrong pwd/usr" ,  connected:0 });
          }
        })

      }else{
        console.log("user does not exist")
        res.status(200).send({ msg: "user does not exist" ,  connected:-1 });

      }})}})




// see if a user is a login 
router.get("/login", (req, res) => {
    if(req.session.user){
      res.send({logedIn : true , user : req.session.user})
    }else{
      res.send({logedIn : false})
    }
  });

module.exports = router;
