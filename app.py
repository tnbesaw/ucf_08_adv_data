import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, MetaData, within_group
from sqlalchemy.pool import StaticPool

from flask import Flask, jsonify

import datetime as dt

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite",
    connect_args={'check_same_thread':False},
    poolclass=StaticPool)

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our connection object
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes"""
    return (
        "Available Routes:<br/><br/>" +
        "<table border=0>" +
        "<tr><td>Precipitation values per station per date:</td><td><a href=""/api/v1.0/precipitation"">/api/v1.0/precipitation</a></td></tr>"+
        "<tr><td>Stations with counts of measurements recorded:</td><td><a href=""/api/v1.0/stations"">/api/v1.0/stations</a></td></tr>"+
        "<tr><td>Temperature observations over the past year:</td><td><a href=""/api/v1.0/tobs"">/api/v1.0/tobs</a></td></tr>"+
        "<tr><td>Temperature results (min, avg, max) since provided date (yyyymmdd):</td><td>/api/v1.0/&lt;start&gt;</td></tr>"+
        "<tr><td>Example:</td><td><a href=""/api/v1.0/20150101"">/api/v1.0/20150101</a></td></tr>"+
        "<tr><td>Temperature results (min, avg, max) between provided dates (yyyymmdd):</td><td>/api/v1.0/&lt;start&gt;/&lt;end&gt;</td></tr>" +
        "<tr><td>Example:</td><td><a href=""/api/v1.0/20150101/20160101"">/api/v1.0/20150101/20160101</a></td></tr>"+
        "</table>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Return a list of all precipitation
    
    # Calculate the date 1 year ago from the last data point in the database
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    print("Last Date: ", last_date.date)

    # Perform a query to retrieve the data and precipitation scores
    query_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    print("Query Date: ", query_date)

    # Query data
    results = (session.query(Measurement.date,
                             func.group_concat(Measurement.prcp.distinct()).label("precipitation"))
            .filter(Measurement.date >= query_date)
            .group_by(Measurement.date)
            .order_by(Measurement.date)
            .all())

    # Create emtpy dict 
    x_result = {}
    
    # Build dict with distinct set of precipitation values on each given day (for each station)
    for result in results:
        x_result.update({result.date: [result.precipitation]})     

    # Return
    return jsonify(x_result)


@app.route("/api/v1.0/stations")
def stations():
    # Return a list of all stations with counts

    # Query data
    results = (session.query(Measurement.station, func.count(Measurement.station).label('cnt'))
            .group_by(Measurement.station)
            .order_by(func.count(Measurement.station).desc())
            .all())    

    # Create emtpy dict 
    x_result = {}

    # Build custom dict for distinct stations ... and counts of measurements per each station
    for result in results:
        x_result.update({result.station: result.cnt})     

    return jsonify(x_result)
    

@app.route("/api/v1.0/tobs")
def tobs():
    # query for the dates and temperature observations from a year from the last data point.
    # return a JSON list of Temperature Observations (tobs) for the previous year.

    # Calculate the date 1 year ago from the last data point in the database
    last_date = (session.query(Measurement.date)
                 .filter(Measurement.station == "USC00519281")     
                 .order_by(Measurement.date.desc()).first())
    print("Last Date: ", last_date.date)

    # Perform a query to retrieve the data and precipitation scores
    query_date = dt.date(2017, 8, 18) - dt.timedelta(days=365)
    print("Query Date: ", query_date)

    results = (session.query(Measurement.date,
                             Measurement.tobs)
               .filter(Measurement.station == "USC00519281")     
               .filter(Measurement.date >= query_date)
               .order_by(Measurement.date)
               .all())

    return jsonify(results)


@app.route("/api/v1.0/<start>")
def temp_start(start):
    
    results = (session.query(func.min(Measurement.tobs), 
                             func.avg(Measurement.tobs), 
                             func.max(Measurement.tobs))
                .filter(Measurement.date >= start)
                .all())

    return jsonify(results)
    

@app.route("/api/v1.0/<start>/<end>")
def temp_start_end(start, end):
    
    results = (session.query(func.min(Measurement.tobs), 
                             func.avg(Measurement.tobs), 
                             func.max(Measurement.tobs))
                .filter(Measurement.date >= start)
                .filter(Measurement.date <= end)
                .all())

    return jsonify(results)
    

if __name__ == '__main__':
    app.run(debug=True)