const express = require("express");
const session = require("express-session");
const cors = require("cors");
const cookieParser = require("cookie-parser")
const bodyParser = require("body-parser");
const passport = require("passport");
const local = require("./strategies/local");

const app = express();

//cors
app.use(cors({
  origin:["http://localhost:8000" , "http://localhost:3000"],
  methods:["GET" , "POST"],
  credentials: true

}))
//cookies 
app.use(cookieParser());

//bodyparser
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
//session
app.use(
  session({
    secret: "secret_code",
    resave: true,
    saveUninitialized: false,
    cookie : {
      expires : 60 * 60 * 24
    }
  })
);


/* les routes*/

const authRouter = require("./routes/Authentification/auth");
const modelsRoute = require("./routes/Models/models");
const usersRoute = require("./routes/Authentification/users");

// Quantity
const historiqueQRoute = require("./routes/Quantity/historique");
const DetailsTrainingQRoute = require("./routes/Quantity/OneTraining");
const DetailsOfMedicationQRoute = require("./routes/Quantity/OneMedication");
const OverviewQRoute = require("./routes/Quantity/Overview");


// PPa
const historiquePRoute = require("./routes/PPa/historique");
const DetailsTrainingPRoute = require("./routes/PPa/OneTraining");
const DetailsOfMedicationPRoute = require("./routes/PPa/OneMedication");
const OverviewPRoute = require("./routes/PPa/Overview");


// AppBar
const NotificationRoute = require("./routes/AppBar/Notification");

/**/

// auth
app.use("/models", modelsRoute);
app.use("/users", usersRoute);
app.use("/auth", authRouter);

// Quantity
app.use("/historiqueQ", historiqueQRoute);
app.use("/DetailsOfTrainingQ", DetailsTrainingQRoute);
app.use("/DetailsOfMedicationQ", DetailsOfMedicationQRoute);
app.use("/overviewQ", OverviewQRoute);

// PPa
app.use("/historiqueP", historiquePRoute);
app.use("/DetailsOfTrainingP", DetailsTrainingPRoute);
app.use("/DetailsOfMedicationP", DetailsOfMedicationPRoute);
app.use("/overviewP", OverviewPRoute);


app.use(passport.initialize());
app.use(passport.session());

// appBar
app.use("/Notification", NotificationRoute);

/*client.execute(query)
  .then(result => console.log('User with username %s', result.rows[1].username));*/

/***************************************************************************************************************** */

//routes
app.get("/", (req, res) => {
  //this is just a test route
  res.json({ toto: "ioi" });
});

app.listen(8000);
