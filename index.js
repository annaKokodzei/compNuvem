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
    console.log("getting the " + numOfMovies + " most popular movies...")
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
    console.log("done")
});

app.get('/actors_from_id/:movie', async (req, res) => {
    var movie = req.params.movie;
    await db.collection('title_basics').findOne({'tconst': movie}).then((result) => {
        console.log("Principal actors from " + result['primaryTitle'] + ":");
    })
    let query = await db.collection('title_principals').find({'tconst': movie, 'category': 'actor'}).toArray();
    let actors = query.map(doc => {
        return {tconst: doc.nconst};
    });
    console.log(actors);
    let something = [];
    for(let i = 0; i < actors.length; i++){
        let names = await db.collection('name_basics').find({'nconst': actors[i].tconst}).toArray();
        let actorNames = names.map(doc => {
            console.log(doc.primaryName);
            return {name: doc.primaryName};
        });
        something = [...something,...actorNames];
        console.log("something: " + something);
    }
    res.header('Content-Type', 'text/html').send("<html><p>"+ JSON.stringify(something)+"<p></p></html>");
});

app.get('/actors_from_title/:movie', async (req, res) => {
    var movie = req.params.movie;
    var movie_id = "";
    await db.collection('title_basics').findOne({'primaryTitle': movie}).then((result) => {
        movie_id = result['tconst'];
        console.log("Principal actors from " + result['primaryTitle'] + ":");
    })
    let query = await db.collection('title_principals').find({'tconst': movie_id, 'category': 'actor'}).toArray();
    let actors = query.map(doc => {
        return {tconst: doc.nconst};
    });
    console.log(actors);
    let something = [];
    for(let i = 0; i < actors.length; i++){
        let names = await db.collection('name_basics').find({'nconst': actors[i].tconst}).toArray();
        let actorNames = names.map(doc => {
            console.log(doc.primaryName);
            return {name: doc.primaryName};
        });
        something = [...something,...actorNames];
        console.log("something: " + something);
    }
    res.header('Content-Type', 'text/html').send("<html><p>"+ JSON.stringify(something)+"<p></p></html>");
});


//db.title_basics.aggregate({$unwind: "$genres"}, {$match: {startYear: 1895}}, {$group: {_id: "$genres", count:{$sum: 1}}}, {$sort: {"count": -1}}, {$limit : 3})
app.get('/category/:year', async(req, res) => {
    const year = parseInt(req.params.year);
    let categor = "";
    console.log(year);
    const query = await db.collection("title_basics").aggregate({$unwind: "$genres"}, {$match: {startYear: year}}, {$group: {_id: "$genres", count:{$sum: 1}}}, {$sort: {"count": -1}}, {$limit : 3}).limit(3)
        .toArray();
    let categories = query.map(doc =>
    {
        return {category:doc._id, counter:doc.count};
    });
    for(let i =0; i < categories.length; i++){
        categor += categories[i].category + "\n";
        console.log(categories[i].category);
    }
    //res.json(query);
    res.header('Content-Type', 'text/html').send("<html><p> Categorias mais produzidas em " +year + " sao " + categor + "</p></html>");

});


app.get('/mostPopularCategory/:year', async(req, res) => {
    const year = parseInt(req.params.year);
    const query = await db.collection('title_basics').aggregate([{$lookup:{from: "ratings", localField: "tconst", foreignField: "tconst", as: "ratings"}}, {$match: {startYear:year}}, {$unwind: "$genres"}, {$group: {_id: {genres: "$genres", rating: "$ratings.averageRating"}, count: {$sum :1}}}, {$unwind: "$_id.rating"}, {$group: {_id: "$_id.genres", ret: {$avg: "$_id.rating"}}}, {$sort: {"ret" : -1}}, {$limit: 1}]).toArray();
    let categories = query.map(doc =>
    {
        console.log(doc);
        return {category:doc._id, rating:doc.ret};
    });
    //res.json(query);
    res.header('Content-Type', 'text/html').send("<html><p> Most successful film category in  " +year + " is " + categories[0].category + "</p></html>");
});



app.get('/director', async (req, res) => {
    const category = { category: "director" };
    let directorList = [];
    var topDirectors = 10;

    await db.collection('title_principals').find(category).forEach((result) => {
        const directorIndex = directorList.findIndex((director) => {
            return director.nconst === result['nconst'];
        });

        if (directorIndex === -1) {
            directorList.push({ nconst: result['nconst'], count: 1 });
        }else{
          directorList[directorIndex].count = directorList[directorIndex].count + 1;
        }
    });

    console.log(directorList);

    const sortedDirectors = directorList.sort((a, b) => {
        return a.count - b.count;
    });

    console.log(sortedDirectors.splice(0, topDirectors));

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
