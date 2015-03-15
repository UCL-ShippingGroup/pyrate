from pyrate.repositories import sql
import logging

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
            ('source_idx', ['source'])
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
            ('source_idx', ['source'])
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
            ('mmsi', 'integer'),
            ('imo', 'integer'),
            ('first_seen', 'timestamp without time zone'),
            ('last_seen', 'timestamp without time zone')
        ],
        'constraint': ['CONSTRAINT imo_list_pkey PRIMARY KEY (mmsi, imo)']
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
        self.tables = [self.clean, self.dirty, self.sources, self.imolist]

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
