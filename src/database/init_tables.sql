CREATE TABLE IF NOT EXISTS vehicles (
	    id SERIAL,
	    vehicle_id VARCHAR(50) NOT NULL,
	    route_id VARCHAR(50),
	    lat DOUBLE PRECISION,
	    lon DOUBLE PRECISION,
	    bearing DOUBLE PRECISION,
	    speed_mph DOUBLE PRECISION,
	    timestamp TIMESTAMP NOT NULL,
	    active BOOLEAN,
	    trip_id VARCHAR(50),
	    direction_id SMALLINT,
	    current_stop_sequence INTEGER,
	    current_status SMALLINT,
	    stop_id INTEGER,
	    occupancy SMALLINT,
	    PRIMARY KEY (id, timestamp)
	) PARTITION BY RANGE (timestamp);
