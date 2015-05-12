from pyrate.repositories import sql
import psycopg2
import logging
try:
    import pandas as pd
except ImportError:
    logging.warn("No pandas found")
    pd = None

EXPORT_COMMANDS = [('status', 'report status of this repository.'),
                   ('create', 'create the repository.'),
                   ('truncate', 'delete all data in this repository.')]

def load(options, readonly=False):
    return AISdb(options, readonly)

class AISdb(sql.PgsqlRepository):

    double_type = 'double precision'

    clean_db_spec = {
        'cols': [
            ('MMSI', 'integer'),
            ('Time', 'timestamp without time zone'),
            ('Message_ID', 'integer'),
            ('Navigational_status', 'integer'),
            ('SOG', double_type),
            ('Longitude', double_type),
            ('Latitude', double_type),
            ('COG', double_type),
            ('Heading', double_type),
            ('IMO', 'integer null'),
            ('Draught', double_type),
            ('Destination', 'character varying(255)'),
            ('Vessel_Name', 'character varying(255)'),
            ('ETA_month', 'integer'),
            ('ETA_day', 'integer'),
            ('ETA_hour', 'integer'),
            ('ETA_minute', 'integer'),
            ('source', 'smallint'),
            ('ID', 'SERIAL PRIMARY KEY')
        ],
        'indices': [
            ('dt_idx', ['Time']),
            ('imo_idx', ['IMO']),
            ('lonlat_idx', ['Longitude', 'Latitude']),
            ('mmsi_idx', ['MMSI']),
            ('msg_idx', ['Message_ID']),
            ('source_idx', ['source']),
            ('mmsi_imo_idx', ['MMSI','IMO'])
        ]
    }

    dirty_db_spec = {
        'cols': [
            ('MMSI', 'integer'),
            ('Time', 'timestamp without time zone'),
            ('Message_ID', 'integer'),
            ('Navigational_status', 'integer'),
            ('SOG', double_type),
            ('Longitude', double_type),
            ('Latitude', double_type),
            ('COG', double_type),
            ('Heading', double_type),
            ('IMO', 'integer null'),
            ('Draught', double_type),
            ('Destination', 'character varying(255)'),
            ('Vessel_Name', 'character varying(255)'),
            ('ETA_month', 'integer'),
            ('ETA_day', 'integer'),
            ('ETA_hour', 'integer'),
            ('ETA_minute', 'integer'),
            ('source', 'smallint'),
            ('ID', 'SERIAL PRIMARY KEY')
        ],
        'indices': [
            ('dt_idx', ['Time']),
            ('imo_idx', ['IMO']),
            ('lonlat_idx', ['Longitude', 'Latitude']),
            ('mmsi_idx', ['MMSI']),
            ('msg_idx', ['Message_ID']),
            ('source_idx', ['source']),
            ('mmsi_imo_idx', ['MMSI','IMO'])
        ]
    }

    sources_db_spec = {
        'cols': [
            ('ID', 'SERIAL PRIMARY KEY'),
            ('filename', 'TEXT'),
            ('ext', 'TEXT'),
            ('invalid', 'integer'),
            ('clean', 'integer'),
            ('dirty', 'integer'),
            ('source', 'integer')
        ]
    }

    imolist_db_spec = {
        'cols': [
            ('mmsi', 'integer NOT NULL'),
            ('imo', 'integer NULL'),
            ('first_seen', 'timestamp without time zone'),
            ('last_seen', 'timestamp without time zone')
        ],
        'constraint': ['CONSTRAINT imo_list_key UNIQUE (mmsi, imo)']
    }

    clean_imo_list = {
        'cols': imolist_db_spec['cols'],
        'constraint': ['CONSTRAINT imo_list_pkey PRIMARY KEY (mmsi, imo)']
    }

    action_log_spec = {
        'cols': [
            ('timestamp', 'timestamp without time zone DEFAULT now()'),
            ('action', 'TEXT'),
            ('mmsi', 'integer NOT NULL'),
            ('ts_from', 'timestamp without time zone'),
            ('ts_to', 'timestamp without time zone'),
            ('count', 'integer NULL')
        ],
        'indices': [
            ('ts_idx', ['timestamp']),
            ('action_idx', ['action']),
            ('mmsi_idx', ['mmsi'])
        ],
        'constraint': ['CONSTRAINT action_log_pkey PRIMARY KEY (timestamp, action, mmsi)']
    }

    def __init__(self, options, readonly=False):
        super(AISdb, self).__init__(options, readonly)
        self.clean = sql.Table(self, 'ais_clean', self.clean_db_spec['cols'],
                               self.clean_db_spec['indices'])
        self.dirty = sql.Table(self, 'ais_dirty', self.dirty_db_spec['cols'],
                               self.dirty_db_spec['indices'])
        self.sources = sql.Table(self, 'ais_sources', self.sources_db_spec['cols'])
        self.imolist = sql.Table(self, 'imo_list', self.imolist_db_spec['cols'], 
                                 constraint=self.imolist_db_spec['constraint'])
        self.extended = AISExtendedTable(self)
        self.clean_imolist = sql.Table(self, 'imo_list_clean', self.clean_imo_list['cols'], constraint=self.clean_imo_list['constraint'])
        self.action_log = sql.Table(self, 'action_log', self.action_log_spec['cols'], self.action_log_spec['indices'], constraint=self.action_log_spec['constraint'])
        self.tables = [self.clean, self.dirty, self.sources, self.imolist, self.extended, self.clean_imolist, self.action_log]

    def status(self):
        print("Status of PGSql database "+ self.db +":")
        for tb in self.tables:
            s = tb.status()
            if s >= 0:
                print("Table {}: {} rows.".format(tb.get_name(), s))
            else:
                print("Table {}: not yet created.".format(tb.get_name()))

    def create(self):
        """Create the tables for the AIS data."""
        for tb in self.tables:
            tb.create()

    def truncate(self):
        """Delete all data in the AIS table."""
        for tb in self.tables:
            tb.truncate()

    def ship_info(self, imo):
        with self.conn.cursor() as cur:
            cur.execute("select vessel_name, MIN(time), MAX(time) from ais_clean where message_id = 5 and imo = %s GROUP BY vessel_name", [imo])
            for row in cur:
                print("Vessel: {} ({} - {})".format(*row))

            cur.execute("select mmsi, first_seen, last_seen from imo_list where imo = %s", [imo])
            for row in cur:
                print("MMSI = {} ({} - {})".format(*row))

    def get_messages_for_vessel(self, imo, from_ts=None, to_ts=None, use_clean_db=False, as_df=False):
        if use_clean_db:
            imo_list = self.imolist
        else:
            imo_list = self.clean_imolist

        where = ["imo = {}"]
        params = [imo]
        #Amended EOK - no time field in this table
        # if not from_ts is None:
        #     where.append("time >= {}")
        #     params.append(from_ts)
        # if not to_ts is None:
        #     where.append("time <= {}")
        #     params.append(to_ts)

        with self.conn.cursor() as cur:
            cur.execute("select mmsi, first_seen, last_seen from {} where {}".format(imo_list.name, ' AND '.join(where)).format(*params))
            msg_stream = None
            # get data for each of this ship's mmsi numbers, and concat
            for mmsi, first, last in cur:
                stream = self.get_message_stream(mmsi, from_ts=first, to_ts=last, use_clean_db=use_clean_db, as_df=as_df)
                if msg_stream is None:
                    msg_stream = stream
                else:
                    msg_stream = msg_stream + stream
            return msg_stream

    def get_message_stream(self, mmsi, from_ts=None, to_ts=None, use_clean_db=False, as_df=False):
        """Gets the stream of messages for the given mmsi, ordered by timestamp ascending"""
        # construct db query
        if use_clean_db:
            db = self.clean
        else:
            db = self.extended
        where = ["mmsi = %s"]
        params = [mmsi]
        if not from_ts is None:
            where.append("time >= %s")
            params.append(from_ts)
        if not to_ts is None:
            where.append("time <= %s")
            params.append(to_ts)
        
        cols_list = ','.join([c[0].lower() for c in db.cols])
        where_clause = ' AND '.join(where)
        sql = "SELECT {} FROM {} WHERE {} ORDER BY time ASC".format(cols_list,
                db.get_name(), where_clause)

        if as_df:
            if pd is None:
                raise RuntimeError("Pandas not found, cannot create dataframe")
            # create pandas dataframe
            with self.conn.cursor() as cur:
                full_sql = cur.mogrify(sql, params).decode('ascii')
            return pd.read_sql(full_sql, self.conn, index_col='time', parse_dates=['time'])

        else:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                msg_stream = []
                # convert tuples from db cursor into dicts
                for row in cur:
                    message = {}
                    for i, col in enumerate(db.cols):
                        message[col[0]] = row[i]
                    msg_stream.append(message)

                return msg_stream

class AISExtendedTable(sql.Table):

    def __init__(self, db):
        super(AISExtendedTable, self).__init__(db, 'ais_extended', 
            AISdb.clean_db_spec['cols'] + [('location', 'geography(POINT, 4326)')],
            AISdb.clean_db_spec['indices'])
    
    def create(self):
        with self.db.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        super(AISExtendedTable, self).create()
        with self.db.conn.cursor() as cur:
            # trigger for GIS location generation
            cur.execute("""CREATE OR REPLACE FUNCTION location_insert() RETURNS trigger AS '
                        BEGIN
                            NEW."location" := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude),4326);
                            RETURN NEW;
                        END;
                        ' LANGUAGE plpgsql;
                        CREATE TRIGGER {0}_gis_insert 
                        BEFORE INSERT OR UPDATE ON {0} FOR EACH ROW EXECUTE PROCEDURE location_insert();
                        """.format(self.name))
        self.db.conn.commit()

    def create_indices(self):
        with self.db.conn.cursor() as cur:
            idxn = self.name.lower() + "_location_idx"
            try:
                logging.info("CREATING GIST INDEX "+ idxn + " on table "+ self.name)
                cur.execute("CREATE INDEX \""+ idxn +"\" ON \"" + self.name +"\" USING GIST(\"location\")")
            except psycopg2.ProgrammingError:
                logging.info("Index "+ idxn +" already exists")
                self.db.conn.rollback()
        super(AISExtendedTable, self).create_indices()

    def drop_indices(self):
        with self.db.conn.cursor() as cur:
            tbl = self.name
            idxn = tbl.lower() + "_location_idx"
            logging.info("Dropping index: "+ idxn + " on table "+ tbl)
            cur.execute("DROP INDEX IF EXISTS \""+ idxn +"\"")
        super(AISExtendedTable, self).drop_indices()
