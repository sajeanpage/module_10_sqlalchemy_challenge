from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask import Flask, jsonify

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///../resources/hawaii.sqlite")
conn = engine.connect()

#tables reflection
Base = automap_base()
Base.prepare(engine, reflect=True)

#save tables and create session
Measurements = Base.classes.measurement
Stations = Base.classes.station

def validate_date(date_text):
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
        return True
    except:
        return False

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        "Welcome to the Climate API!<br/><br/>"
        "Available Routes:<br/>"
        " /api/v1.0/precipitation<br/>"
        " /api/v1.0/stations<br/>"
        " /api/v1.0/tobs<br/>"
        " /api/v1.0/{start}<br/>"
        " /api/v1.0/{start}/{end}"
    )


@app.route("/api/v1.0/stations")
def climate_stations():
    session = Session(engine)

    stationlist = []
    station_results = session.query(Stations).all()
    session.close()
 
    for station in station_results:
        print(station.station)
        stationlist.append(station.station)

    return jsonify(stationlist)


@app.route("/api/v1.0/precipitation")
def measurements():
    session = Session(engine)
    newest_measurement_date_string = session.query(Measurements, func.max(Measurements.date).label('date')).first().date
    year_back = datetime.strptime(newest_measurement_date_string,'%Y-%m-%d') + relativedelta(months=-12)

    measurementlist = []
    measurement_results = session.query(Measurements).filter(Measurements.date >= year_back)
    session.close()

    for measurement in measurement_results:
        measurement_dict = {}
        measurement_dict[measurement.date] = measurement.prcp
        measurementlist.append(measurement_dict)

    return jsonify(measurementlist)


@app.route("/api/v1.0/tobs")
def get_tobs():
    session = Session(engine)
    newest_measurement_date_string = session.query(Measurements, func.max(Measurements.date).label('date')).first().date
    year_back = datetime.strptime(newest_measurement_date_string,'%Y-%m-%d') + relativedelta(months=-12)

    most_active_station = session.query(Measurements.station, func.count(Measurements.station)).group_by(Measurements.station)\
        .order_by(func.count(Measurements.station).desc()).first()

    most_active_measurementlist = []
    most_active_stmt = session.query(Stations, Measurements).filter(Stations.station == Measurements.station).filter(Stations.station == most_active_station[0]).filter(Measurements.date >= year_back).statement  
    most_active_measurement_results = session.execute(most_active_stmt)

    for station, measurement  in most_active_measurement_results:
        most_active_measurement_dict = {}
        most_active_measurement_dict["date"] = measurement.date
        most_active_measurement_dict["station"] = station.station
        most_active_measurement_dict["name"] = station.name
        most_active_measurement_dict["prcp"] = measurement.prcp
        most_active_measurement_dict["tobs"] = measurement.tobs
        most_active_measurementlist.append(most_active_measurement_dict)

    session.close()

    return jsonify(most_active_measurementlist)


@app.route("/api/v1.0/<start>")
def get_temps_with_start(start=None):

    has_dates = validate_date(start)

    if has_dates:
        session = Session(engine)

        tobs_measures = session.query(func.min(Measurements.tobs).label('tmin'), func.avg(Measurements.tobs).label('tavg'),\
            func.max(Measurements.tobs).label('tmax')).filter(Measurements.date >= start).first()

        dated_measurement_dict = {}
        dated_measurement_dict["TMIN"] = tobs_measures.tmin
        dated_measurement_dict["TAVG"] = tobs_measures.tavg
        dated_measurement_dict["TMAX"] = tobs_measures.tmax

        session.close()

        rtn = dated_measurement_dict
    else:
        rtn = "Incorrect data format, should be YYYY-MM-DD"

    return jsonify(rtn)


@app.route("/api/v1.0/<start>/<end>")
def get_temps_with_end(start=None, end=None):

    has_dates = validate_date(start) and validate_date(end)

    if has_dates:
        session = Session(engine)

        tobs_measures = session.query(func.min(Measurements.tobs).label('tmin'), func.avg(Measurements.tobs).label('tavg'),\
            func.max(Measurements.tobs).label('tmax')).filter(Measurements.date >= start).filter(Measurements.date <= end).first()

        dated_measurement_dict = {}
        dated_measurement_dict["TMIN"] = tobs_measures.tmin
        dated_measurement_dict["TAVG"] = tobs_measures.tavg
        dated_measurement_dict["TMAX"] = tobs_measures.tmax

        session.close()

        rtn = dated_measurement_dict
    else:
        rtn = "Incorrect data format, should be YYYY-MM-DD"

    return jsonify(rtn)


if __name__ == "__main__":
    app.run(debug=True)




