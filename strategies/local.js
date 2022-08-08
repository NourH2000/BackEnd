const LocalStrategy = require("passport-local");
const passport = require("passport");
const db = require("../database");

const cassandra = require("cassandra-driver");
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "user",
});

passport.serializeUser((user, done) => {
  //store the id in session
  done(null, user.username);
});

passport.deserializeUser(async (username, done) => {
  const query = "SELECT * FROM user WHERE  username= ?  ALLOW FILTERING ";
  client
    .execute(query, [username])
    .then((result) => {
      var user = result;
      if (user[0]) {
        done(null, user[0]);
      }
    })
    .catch((err) => {
      done(err, null);
    });
});

passport.use(
  new LocalStrategy(async (username, password, done) => {
    const query = "SELECT * FROM user WHERE  username= ?  ALLOW FILTERING ";
    client
      .execute(query, [username])
      .then((result) => {
        var user = result.rows;
        //user not found in database
        if (user.length === 0) {
          done(null, false);
        } else {
          //user found in database => compare password
          if (user[0].pwd === password) {
            done(null, user[0]);
          } else {
            done(null, false);
          }
        }
        console.log(user[0].username);
      })
      .catch((err) => {
        done(err, false);
      });
  })
);
