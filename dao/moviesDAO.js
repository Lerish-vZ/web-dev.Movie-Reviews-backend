import mongodb from "mongodb";
const ObjectId = mongodb.ObjectID;

let movies; //stores refernece to the database

export default class MoviesDAO {
  static async injectDB(conn) {
    //injectDB (async method) gets called as soon as server starts. Provides database reference to movies.
    if (movies) {
      return;
    }
    try {
      //if reference already exists we return...(next line)
      movies = await conn.db(process.env.MOVIEREVIEWS_NS).collection("movies");
    } catch (e) {
      console.error(`unable to connect in MoviesDAO: ${e}`);
    }
  }
  static async getMovies({
    // default filter
    filters = null,
    page = 0,
    moviesPerPage = 20, // will only get 20 movies at once
  } = {}) {
    let query;
    if (filters) {
      if(filters.hasOwnProperty('title')) {
        query = { $text: { $search: filters["title"] } };
      } else if (filters.hasOwnProperty('rated')) {
        query = { rated: { $eq: filters["rated"] } };
      }
    }
    let cursor;
    try {
      cursor = await movies
        .find(query)
        .limit(moviesPerPage)
        .skip(moviesPerPage * page);
      const moviesList = await cursor.toArray();
      const totalNumMovies = await movies.countDocuments(query);
      return { moviesList, totalNumMovies };
    } catch (e) {
      console.error(`Unable to issue find command, ${e}`);
      return { moviesList: [], totalNumMovies: 0 };
    }
  }

  static async getRatings() {
    let ratings = [];

    try{
      ratings = await movies.distinct('rated'); //We use movies.distinct to get all the distinct rated values from the movies collection.
      return ratings;
    }
    catch(e){
      console.error(`unable to get ratings, ${e}`);
      return ratings;
    }
  }

  static async getMovieById(id){
    try{
      return await movies.aggregate([ //We use aggregate to provide a sequence of data aggregation operations.
        {
          $match: {
            _id: new ObjectId(id)
          }
        },
        { $lookup: //syntax
          {
            from: 'reviews', // <collection to join>
            localField: '_id', // <field from the input document>
            foreignField: 'movie_id', // <field from the documents of the "form" collection>
            as: 'reviews', // <output array field
          } //This finds all the reviews with the specific movie id and returns the specific movie together with the reviews in an array.
        }
      ]).next();
    }
    catch(e){
      console.error(`something went wrong in getMovieById: ${e}`);
      throw e;
    }
  }
}
