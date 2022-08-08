var { PythonShell } = require("python-shell");

const pythonShellScript = (path, options) => {
  PythonShell.run(path, options, function (err, results) {
    if (err) throw err;
    console.log(" the model has been called");
    console.log(results);
  });
};

module.exports = { pythonShellScript };

/* */
