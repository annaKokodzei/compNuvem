const express = require('express')
const app = express()
var bodyParser = require("body-parser");
const MongoClient = require('mongodb').MongoClient

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const port = 3000
const url = "mongodb://localhost:27017"
const client = new MongoClient(url);
var db;

//conecta ao mongo
client.connect((err) => {
  if (err)  {
      return console.log(err);
  }

  db = client.db('imdb');
  //caso não dê erro, arranca com a api
  app.listen(port, () => {
    console.log(`Example app listening on port ${port}!`);
  })
})

//API
app.get('/actor', (req, res) => {
    const name = req.query.name;
    const birthYear = req.query.birthYear;
    const deathyear =  req.query.deathYear;
    const known =  req.query.known;
    const profession =  req.query.profession;

    db.collection('name_basics').find({
      name: name,
      birthYear: birthYear,
      deathyear: deathyear,
      knownForTitles: known,
      primaryProfession: profession
    }).toArray(function(err, results) {
      console.log(results);
    });
});

app.get('/actor/:name', (req, res) => {
    const name = req.params.name;
    db.collection('name_basics').find({primaryName: name}).toArray(function(err, results) {
        console.log(results);
        delete results[0]['_id']
        delete results[0]['nconst']
        res.json(results[0]);
    });
});

app.post('/actor/:name', (req, res) => {
    const name = req.params.name;
    const primaryName = req.body.primaryName;

    db.collection('name_basics').updateOne({ primaryName: name },{primaryName: primaryName}).then(result => {
      console.log(result);
    });
});




app.get('/maxrating', (req, res) => {
    const query = db.collection('title_ratings').find().sort({'averageRating':-1}).limit(1);
    query.forEach((doc) => {
        let tconst = doc['tconst'];
        let ratingValue = doc['averageRating'];
        const getTitle = db.collection('title_basics').find({"tconst": tconst});
        getTitle.forEach((docs) => {
            film = docs['primaryTitle'];
            res.header('Content-Type', 'text/html').send("<html><p>Film with the highest rating is: " + film +". The rating values is: "+ratingValue+"</p></html>");
            res.end();
        });
    });
    console.log("maxRating sent")
});

app.get('/popular/:num', (req,res) => {
    var count = 0;
    var numOfMovies = parseInt(req.params.num);
    if(numOfMovies > 0) {
        const query = db.collection('title_ratings').find().sort({'numVotes': -1}).limit(numOfMovies);
        var movie_titles = []
        query.forEach((results) => {
            movie_id = results['tconst']
            const getTitle = db.collection('title_basics').find({"tconst": movie_id});
            getTitle.forEach((doc) => {
                console.log(doc['primaryTitle'])
                movie_titles.push(doc['primaryTitle'])
                count++;
                if(count === numOfMovies) res.json(movie_titles);
            })
        })
    } else {
        res.json([])
    }
});


//-----------------------------------------------UPDATES-------------------------------------------------
//Acho que isto devia ser removido
app.get('/update', (req, res) => {
    const collection = 'title_principals';
    const type = 'characters';
    var ops = [];
    var count = 0;
    var query = db.collection(collection).find({ [type]: { "$not": {"$type":"array"}, "$type": 2} });
    var total = 0;

    /*  query.toArray().then((data) => {
        total = data.length;

      });
  */
    query.forEach(doc => {
        if (typeof doc[type] !== 'string') {
            console.log("dei erro");
            console.log(doc[type]);
            return;
        }

        var children = doc[type].replace(/^\s+|\s+$/gm,'');
        children = children.replace(/\s*,\s*/ig, ',');
        children = children.replace(/\s\s+/g, ',');
        //console.log(children);
        if (children === "" || children === "\N") {
            children = [];
        } else {
            const childrens = children.split(',');
            children = childrens[1];
        }

        ops.push({
            "updateOne": {
                "filter": { "_id": doc._id },
                "update": { "$set": { [type]: children } }
            }
        });
        count++;

        if ( ops.length >= 100000 ) {
            console.log("I am pushing");
            db.collection(collection).bulkWrite(ops);
            console.log("I have pushed");
            ops = [];
            //console.log(count);
        }
    });

 /*   if ( ops.length > 0 ) {
        db.collection(collection).bulkWrite(ops);
        ops = [];
        console.log(count);
    }
*/
});


app.get('/update/anotherfield', (req, res) => {
    const collection = 'actors';
    const type = 'knownForTitles';
    var ops = [];
    var count = 0;
    var query = db.collection(collection).find({ [type]: { "$not": {"$type":"array"}, "$type": 2} });
    var total = 0;

    /*  query.toArray().then((data) => {
        total = data.length;

      });
  */
    query.forEach(doc => {
        if (typeof doc[type] !== 'string') {
            console.log("dei erro");
            console.log(doc[type]);
            return;
        }

        var children = doc[type].replace(/^\s+|\s+$/gm,'');
        children = children.replace(/\s*,\s*/ig, ',');
        children = children.replace(/\s\s+/g, ',');
        //console.log(children);
        if (children === "" || children === "\\N") {
            console.log("I am at here");
            children = [];
        } /*else {
            childrens = children.split('["');
            childs= childrens[1].split('"]');

            children = childs[0].split(',');
            console.log(children);
            //children = JSON.parse(children);
        }*/

        ops.push({
            "updateOne": {
                "filter": { "_id": doc._id },
                "update": { "$set": { [type]: children } }
            }
        });
        count++;

        if ( ops.length >= 100000) {
            console.log("I am pushing");
            db.collection(collection).bulkWrite(ops);
            console.log("I have pushed");
            ops = [];
            //console.log(count);
        }
    });

/*    if ( ops.length > 0 ) {
        db.collection(collection).bulkWrite(ops);
        ops = [];
        console.log(count);
    }
*/
});

app.get('/update/titleakas', (req, res) => {
    const collection = 'title_akas';
    const type = 'attributes';
    var ops = [];
    var count = 0;
    let query = db.collection(collection).find({ [type]: {"$type":"array"}});
    let total = 0;
/*
      query.toArray().then((data) => {
        total = data.length;
        console.log("total" + total);
      });
*/
    query.forEach((doc) => {
        if (typeof doc[type] === 'string') {
            console.log("dei erro");
            console.log(doc[type]);
            return;
        }
        else {
            let children = doc[type];
            //var children = doc[type].replace(/^\s+|\s+$/gm,'');
            //children = children.replace(/\s*,\s*/ig, ',');
            //children = children.replace(/\s\s+/g, ',');
            //console.log(children);
            if (children[0] === "\\N") {
                //console.log("I am at here");
                children = [];
                //console.log("children" + children);
            }
            ops.push({
                "updateOne": {
                    "filter": { "_id": doc._id },
                    "update": { "$set": { [type]: children } }
                }
            });
            count++;

            if ( ops.length >= 10000) {
                console.log("I am pushing");
                db.collection(collection).bulkWrite(ops).then(r=>total+=1).catch(err=>{console.log(err);throw err});
                console.log("I have pushed");
                ops = [];
                //console.log(count);
            }
            console.log("tt",total);
        }

    });

    if ( ops.length > 0 ) {
        db.collection(collection).bulkWrite(ops);
        ops = [];
        console.log(count);
    }

});
