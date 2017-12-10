import psycopg2
from pyiso import client_factory, nyiso
import pandas as pd
np = pd.np
import datetime
from io import StringIO
import time
import math
from random import randint
from os import system

class dbfuncs(object):
    def __init__(self, database, verbose = True):
        self.options = {}
        self.options['verbose'] = verbose

        self.con = psycopg2.connect(**database)
        self.cur = self.con.cursor()

    def _set_up_structure(self, market):
        if self.options['verbose']:
            print 'Checking database'

        self.cur.execute("""select exists(
                        select * from information_schema.tables
                        where table_name='nodes')""")
        table = self.cur.fetchone()[0]

        if not table:
            if self.options['verbose']:
                print 'Creating nodes table'

            self.cur.execute("""
                create table nodes
                (
                id serial PRIMARY KEY
                ,market_id int
                ,iso_node_id int
                ,name varchar
                ,lat real
                ,lon real
                )
                """)

        self.cur.execute("""select exists(
                        select * from information_schema.tables
                        where table_name='isos')""")
        table = self.cur.fetchone()[0]

        if not table:
            if self.options['verbose']:
                print 'Creating ISO table'

            self.cur.execute("""
                create table isos
                (
                id serial PRIMARY KEY
                , name varchar
                )
                """)

        self.cur.execute("""select exists(
                        select * from isos
                        where name='{0}')""".format(market))
        table = self.cur.fetchone()[0]

        if not table:
            if self.options['verbose']:
                print 'Inserting {0} to ISO table'.format(market)

            self.cur.execute("""insert into isos (name)
                            values ('{0}')""".format(market))

        for p in ['dahr', 'rt5m']:
            self.cur.execute("""select exists(
                            select * from information_schema.tables
                            where table_name='lmp_{1}_{0}')""".format(market, p))
            table = self.cur.fetchone()[0]

            if not table:
                if self.options['verbose']:
                    print 'Creating lmp table for {1} {0}'.format(market, p)

                self.cur.execute("""
                    create table lmp_{1}_{0}
                    (
                    id int
                    , datatime timestamp without time zone
                    , loss real
                    , congestion real
                    , energy real
                    , lmp real
                    , CONSTRAINT lmp_{1}_{0}_pkey PRIMARY KEY(id, datatime)
                    )
                    """.format(market))

            if market == 'nyiso':
                if p == 'dahr':
                    self.cur.execute("""select exists(
                    select * from information_schema.tables
                    where table_name='anc_dahr_{0}')""".format(market))
                    table = self.cur.fetchone()[0]

                    if not table:
                        self.cur.execute("""
                                create table anc_{1}_{0}
                                (
                                datatime timestamp without time zone
                                ,name varchar
                                ,iso_node_id int
                                ,spin_10 real
                                ,spin_10_non_synch real
                                ,op_res_30 real
                                ,reg_cap real
                                , CONSTRAINT anc_{1}_{0}_pkey PRIMARY KEY(datatime, name)
                                )
                                """.format(market, p))
                elif p == 'rt5m':
                    self.cur.execute("""select exists(
                    select * from information_schema.tables
                    where table_name='anc_rt5m_{0}')""".format(market))
                    table = self.cur.fetchone()[0]

                    if not table:
                        self.cur.execute("""
                                create table anc_{1}_{0}
                                (
                                datatime timestamp without time zone
                                ,name varchar
                                ,iso_node_id int
                                ,spin_10 real
                                ,spin_10_non_synch real
                                ,op_res_30 real
                                ,reg_cap real
                                ,reg_mov real
                                , CONSTRAINT anc_{1}_{0}_pkey PRIMARY KEY(datatime, name)
                                )
                                """.format(market, p))
            else:
                print "No spec for that market's anc table"

            self.con.commit()

        if self.options['verbose']:
            print 'Database is successfully configured'

    def _get_rto_id(self, market):
        self.cur.execute("""select id
                        from isos
                        where name = '{0}'""".format(market))
        table = self.cur.fetchone()[0]

        return table

    def _insert_nodes(self, df, market_id):
        if self.options['verbose']:
            print 'Inserting nodes'

        unique = df.drop_duplicates(subset = ['iso_node_id', 'name'])

        if ('lat' in df.columns) and ('lon' in df.columns):
            for idx, row in unique.iterrows():
                if math.isnan(row['lat']):
                    row['lat'] = 'null'
                if math.isnan(row['lon']):
                    row['lon'] = 'null'

                self.cur.execute("""
                            INSERT INTO nodes
                                (market_id, iso_node_id, name, lat, lon)
                            SELECT {0}, {1}, '{2}', {3}, {4}
                            WHERE
                                NOT EXISTS (
                                    SELECT id
                                    FROM nodes
                                    WHERE market_id = {0}
                                    AND name = '{2}'
                                );
                            """.format(market_id, row['iso_node_id'], row['name'], row['lat'], row['lon']))

        else:
            for idx, row in unique.iterrows():
                self.cur.execute("""
                        INSERT INTO nodes
                            (market_id, iso_node_id, name)
                        SELECT {0}, {1}, '{2}'
                        WHERE
                            NOT EXISTS (
                                SELECT id
                                FROM nodes
                                WHERE market_id = {0}
                                AND name = '{2}'
                            );
                        """.format(market_id, row['iso_node_id'], row['name']))

        self.con.commit()

    def _get_all_nodes(self, market_id):
        self.cur.execute("""select id, iso_node_id, name
                        from nodes
                        where market_id={0}""".format(market_id))

        node_data = self.cur.fetchall()
        node_data = pd.DataFrame(node_data)
        node_data.columns = ['id', 'iso_node_id', 'name']

        return node_data

    def _insert_lmp_data(self, df, market_id, market, product):
        if self.options['verbose']:
            print 'Inserting lmp data'

        if self.options['verbose']:
            print '    Retrieving node data and joining'

        node_data = self._get_all_nodes(market_id)
        df = df.merge(node_data, on = ['name', 'iso_node_id'])
        df.drop(['name', 'iso_node_id'], axis = 1, inplace = True)

        if self.options['verbose']:
            print '    Looping through and inserting lmp data'

        r = randint(1, 1000000000000)
        self.cur.execute("""
                create temp table temp_lmp_{1}_{0}_{2}
                (
                id int
                , datatime timestamp without time zone
                , loss real
                , congestion real
                , energy real
                , lmp real
                )
                """.format(market, product, r))

        self.con.commit()

        if self.options['verbose']:
            print '    Checking Duplicates'
        df.drop_duplicates(subset = ['id', 'datatime'], inplace = True)

        if self.options['verbose']:
            print '    Writing file'
        f = '/tmp/temp_insert_{0}.csv'.format(r)
        #create file
        df.to_csv(f, index = False, chunksize = 100000
            , columns=['id', 'datatime', 'loss', 'congestion', 'energy', 'lmp'])

        if self.options['verbose']:
            print '    Copying'
        #copy file in
        self.cur.execute("""
            COPY temp_lmp_{1}_{0}_{3}
            FROM '{2}'
            DELIMITER ',' CSV HEADER
            """.format(market, product, f, r))

        system('rm {0}'.format(f))

        """
        id = df['id'].values
        datatime = df['datatime'].values
        loss = df['loss'].values
        congestion = df['congestion'].values
        lmp = df['lmp'].values

        for i in xrange(0,len(df)):
            energy = lmp[i] - congestion[i] - loss[i]
            self.cur.execute("""
        #            INSERT INTO temp_lmp_{1}_{0}
        #                (id, datatime, loss, congestion, energy, lmp)
        #            SELECT {2}, '{3}', {4}, {5}, {6}, {7};
        #            """.format(market, product, id[i], datatime[i], loss[i], congestion[i], energy, lmp[i]))
        """self.con.commit()
        """

        if self.options['verbose']:
            print '    Create Index'

        self.cur.execute("""
                alter table temp_lmp_{1}_{0}_{2}
                add PRIMARY KEY (id, datatime)
                """.format(market, product, r))

        if self.options['verbose']:
            print '    Inserting where not exists'

        self.cur.execute("""
                    INSERT INTO lmp_{1}_{0}
                    (id, datatime, loss, congestion, energy, lmp)
                    SELECT tmp.id, tmp.datatime, tmp.loss, tmp.congestion, tmp.energy, tmp.lmp
                    FROM temp_lmp_{1}_{0}_{2} tmp
                    LEFT JOIN lmp_{1}_{0} datatbl
                    ON tmp.id = datatbl.id
                    AND tmp.datatime = datatbl.datatime
                    WHERE datatbl.datatime is NULL
                    """.format(market, product, r))
        self.con.commit()

        #Cleans up the temp table
        self.cur.execute("""drop table temp_lmp_{1}_{0}_{2}""".format(market, product, r))
        self.con.commit()

    def _get_node_list(self, market):
        rto_id = self._get_rto_id(market)
        nodes = self._get_all_nodes(rto_id)

        return nodes

    def _get_lmp_data(self, market, id, product = 'rt5m', year = None):
        if year is not None:
            self.cur.execute("""select datatime, lmp
                            from lmp_{3}_{0}
                            where id={1}
                            and date_part('year', datatime) = {2}
                            order by datatime""".format(market, id, year, product))
        else:
            self.cur.execute("""select datatime, lmp
                            from lmp_{2}_{0}
                            where id={1}
                            order by datatime""".format(market, id, product))

        node_data = self.cur.fetchall()
        node_data = pd.DataFrame(node_data)
        node_data.columns = ['datatime', 'lmp']

        return node_data

    def _get_anc_data(self, market, product, year = None):
        if year is not None:
            self.cur.execute("""select *
                            from anc_{1}_{0}
                            where date_part('year', datatime) = {2}
                            order by datatime""".format(market, product, year))
        else:
            self.cur.execute("""select datatime, lmp
                            from lmp_{1}_{0}
                            order by datatime""".format(market, product))

        anc_data = self.cur.fetchall()
        anc_data = pd.DataFrame(anc_data)

        if market == 'nyiso':
            if product == 'rt5m':
                anc_data.columns = ['datatime', 'name', 'iso_node_id', 'spin_10', 'spin_10_non_synch', 'op_res_30', 'reg_cap', 'reg_mov']
            elif product == 'dahr':
                anc_data.columns = ['datatime', 'name', 'iso_node_id', 'spin_10', 'spin_10_non_synch', 'op_res_30', 'reg_cap']

        return anc_data


    def _insert_anc_data(self, df, market, product):
        new_month_flag = True

        for idx, row in df.iterrows():
            dt = row['datatime']
            name = row['name']
            iso_node_id = row['iso_node_id']
            spin_10 = row['spin_10']
            spin_10_non_synch = row['spin_10_non_synch']
            op_res_30 = row['op_res_30']
            reg_cap = row['reg_cap']

            if dt.day == 1 and new_month_flag:
                print 'Inserting Month beginning {0}'.format(dt)
                new_month_flag = False

            if dt.day == 2:
                new_month_flag = True

            if product == 'rt5m':
                reg_mov = row['reg_move']

            if product == 'rt5m':
                self.cur.execute("""INSERT INTO anc_{0}_{1}
                                (datatime, name, iso_node_id, spin_10, spin_10_non_synch, op_res_30, reg_cap, reg_mov)
                            SELECT '{2}', '{3}', {4}, {5}, {6}, {7}, {8}, {9}
                            WHERE
                                NOT EXISTS (
                                    SELECT datatime FROM anc_{0}_{1} WHERE datatime = '{2}' AND name = '{3}'
                                );""".format(product, market, dt, name, iso_node_id, spin_10, spin_10_non_synch, op_res_30, reg_cap, reg_mov))

            elif product == 'dahr':
                self.cur.execute("""INSERT INTO anc_{0}_{1}
                                (datatime, name, iso_node_id, spin_10, spin_10_non_synch, op_res_30, reg_cap)
                            SELECT '{2}', '{3}', {4}, {5}, {6}, {7}, {8}
                            WHERE
                                NOT EXISTS (
                                    SELECT datatime FROM anc_{0}_{1} WHERE datatime = '{2}' AND name = '{3}'
                                );""".format(product, market, dt, name, iso_node_id, spin_10, spin_10_non_synch, op_res_30, reg_cap))

        self.con.commit()

class pyisodb(object):
    def __init__(self, database, verbose = True):
        self.options = {}
        self.options['verbose'] = verbose
        self.db = dbfuncs(database)

        self.lmp_get = {
        'nyiso': self.lmp_nyiso
        ,'caiso': self.lmp_caiso
        #,'neiso': lmp_neiso
        }

        self.anc_get = {
        'nyiso': self.anc_nyiso
        }

        self.deltatime = {'nyiso': DeltaTimes.nyiso
                    ,'caiso': DeltaTimes.caiso}

    def download_lmp_data(self, market, start_time, end_time, product = None):
        self.db._set_up_structure(market)
        market_id = self.db._get_rto_id(market)

        while start_time < end_time:
            i_end_time = self.deltatime[market](start_time)
            if self.options['verbose']:
                print 'Beginning {0} to {1} for {2}'.format(start_time, i_end_time, market)
            
            tries = 0
            while True:
                data = self.lmp_get[market](start_time, i_end_time, product)
                if len(data) > 0:
                    self.db._insert_nodes(data, market_id)
                    self.db._insert_lmp_data(data, market_id, market, product)
                    break
                else:
                    print 'No data for {0} to {1} for {2} on try {3}'.format(start_time, i_end_time, market, tries)
                    if tries > 5:
                        print 'WARNING: Skipping {0} to {1} for {2}'.format(start_time, i_end_time, market)
                    else:
                        time.sleep(10)
                        tries += 1

            start_time = i_end_time

    def lmp_nyiso(self, start_time, end_time, product):
        if self.options['verbose']:
            'Retrieving NYISO data'

        end_time = end_time - datetime.timedelta(seconds = 1)

        nyiso = client_factory('NYISO')
        data = nyiso.get_lmp(data = 'lmp', node_id = 'ALL', start_at = start_time, end_at = end_time, market = product)

        data = pd.DataFrame(data)
        data.drop(['ba_name', 'freq', 'lmp_type', 'market'], axis = 1, inplace = True)
        data.rename(columns = {'PTID': 'iso_node_id'
                        ,'node_id': 'name'
                        ,'timestamp': 'datatime'}, inplace = True)

        return data

    def lmp_caiso(self, start_time, end_time, product):
        if self.options['verbose']:
            'Retrieving CAISO data'

        caiso = client_factory('CAISO')
        node = 'ALL'
        #node = 'SLAP_PGP2-APND'

        data = caiso.get_lmp_as_dataframe(node, start_at = start_time, end_at = end_time, market_run_id = 'RTM', lmp_only = False)
        if len(data) > 0:
            data['datatime'] = data.index
            data.rename(columns = {'NODE': 'name'}, inplace = True)

            data.set_index(['datatime', 'name'], inplace = True)
            data.drop(['OPR_DT', 'NODE_ID_XML', 'NODE_ID'
                , 'MARKET_RUN_ID', 'LMP_TYPE', 'INTERVALENDTIME_GMT'
                , 'PNODE_RESMRID', 'GRP_TYPE', 'POS', 'OPR_INTERVAL'
                , 'GROUP', 'OPR_HR'], axis = 1, inplace = True)

            df_cong = data[data['XML_DATA_ITEM'] == 'LMP_CONG_PRC'].copy()
            df_cong.rename(columns = {'MW': 'congestion'}, inplace = True)

            df_ene = data[data['XML_DATA_ITEM'] == 'LMP_ENE_PRC'].copy()
            df_ene.rename(columns = {'MW': 'energy'}, inplace = True)

            df_loss = data[data['XML_DATA_ITEM'] == 'LMP_LOSS_PRC'].copy()
            df_loss.rename(columns = {'MW': 'loss'}, inplace = True)

            df_lmp = data[data['XML_DATA_ITEM'] == 'LMP_PRC'].copy()
            df_lmp.rename(columns = {'MW': 'lmp'}, inplace = True)

            df = df_cong.join(df_ene['energy'])
            df = df.join(df_loss['loss'])
            df = df.join(df_lmp['lmp'])
            df.drop(['XML_DATA_ITEM'], axis = 1, inplace = True)
            df.reset_index(drop = False, inplace = True)

            df['iso_node_id'] = -99

            return df
        else:
            return []

    def anc_nyiso(self, market, start_time, end_time, product):
        self.db._set_up_structure(market)

        nyiso_anc = nyiso.NYISO_Anc()
        data = nyiso_anc.get_data(start_at = start_time, end_at = end_time, product = product)       #rt5m or dahr

        return data

    def download_anc_data(self, market, start_time, end_time, product):
        """
        Market can be rt or dam
        """
        data = self.anc_get[market](market, start_time, end_time, product) #returns a list of dfs

        for d in data:
            self.db._insert_anc_data(d, market, product)

    def close(self):
        self.db.con.close()

class DeltaTimes(object):
    @staticmethod
    def nyiso(start_time):
        month = start_time.month
        year = start_time.year

        month += 1
        if month == 13:
            month = 1
            year += 1

        end_time = start_time.replace(month=month, year=year)

        return end_time

    @staticmethod
    def caiso(start_time):
        end_time = start_time + datetime.timedelta(hours = 1)
        return end_time













        