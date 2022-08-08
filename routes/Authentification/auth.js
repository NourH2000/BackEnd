const express = require("express");
const db = require("../../database");
const passport = require("passport");
const router = express.Router();

// test request
router.get("/", (req, res) => {
  res.json({ toto: "authentification" });
});

// test request
router.post("/login", passport.authenticate("local"), (req, res) => {
  res.send(200);
});
module.exports = router;
