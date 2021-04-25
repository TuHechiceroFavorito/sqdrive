import sqlite3
import gspread
from gspread_formatting import BooleanCondition, DataValidationRule, set_data_validation_for_cell_range
from oauth2client.service_account import ServiceAccountCredentials
import logging
import numpy as np
from time import asctime, localtime, sleep

waiting_time = 2
wait_exceed = 20

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

class DBuilder:
    def __init__(self, urls, creds, dbname=':memory:'):
        creds_set = ServiceAccountCredentials.from_json_keyfile_name(creds, scope)
        self.client = gspread.authorize(creds_set)
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname, check_same_thread=False)
        self.c = self.conn.cursor()
        self.urls = urls
        self.reset()

    def get_sheets(self, db, mode=False, sheet=None, data=True, tab=None, formulas=False):
        if formulas:
            render = 'FORMULA'
        else:
            render = 'FORMATTED_VALUE'

        if mode == 'add':
            logger.debug(f'Adding tab to "{db}"')
            try:
                sheet.add_worksheet(tab)
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheet.add_worksheet(tab)
                sleep(waiting_time)

            logger.debug(f'Tab added to "{db}"')

        else:
            logger.debug(f'Opening "{db}"')
            try:
                sheets = self.client.open_by_url(self.urls[db])
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheets = self.client.open_by_url(self.urls[db])
                sleep(waiting_time)

            logger.debug(f'"{db}" opened')


        if mode == False:
            return sheets

        elif mode == 'template':
            logger.debug(f'Opening template for "{db}"')
            try:
                sheet = sheets.sheet1
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheet = sheets.sheet1
                sleep(waiting_time)
            logger.debug(f'Template for "{db}" opened')

            if data == True:
                try:
                    sheet = sheet.get_all_values(value_render_option=render)
                    sleep(waiting_time)
                except:
                    logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                    sleep(wait_exceed)
                    sheet = sheet.get_all_values(value_render_option=render)
                    sleep(waiting_time)
                logger.debug(f'Data from template for "{db}" retrieved')
                
            

        elif mode == 'work' or mode == 'data':
            logger.debug(f'Getting last tab for "{db}"')
            try:
                tabs = len(sheets.worksheets()) - 1
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                tabs = len(sheets.worksheets()) - 1
                sleep(waiting_time)
            try:
                sheet = sheets.get_worksheet(tabs)
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheet = sheets.get_worksheet(tabs)
                sleep(waiting_time)
            
            logger.debug(f'Last tab for "{db}" opened')
        
            if mode == 'data':
                logger.debug(f'Getting all values from "{db}"')
                try:
                    sheet = sheet.get_all_values(value_render_option=render)
                    sleep(waiting_time)
                except:
                    logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                    sleep(wait_exceed)
                    sheet = sheet.get_all_values(value_render_option=render)
                    sleep(waiting_time)

                logger.debug(f'Values retrieved "{db}"')
                        
        return sheet

    def create_table(self, db, num=False):
        logger.debug(f'Creating table "{db}"')
        data = self.get_sheets(db, mode='data')

        for i in range(len(data[0])):
            if num:
                data[0][i] = i
            else:
                if data[0][i] == '':
                    data[0][i] = f'empty{i}'

        keys = data[0]
        try:
            with self.conn:
                self.c.execute(" CREATE TABLE %s (id integer PRIMARY KEY NOT NULL)" %(db))
                for key in keys:
                    self.c.execute("ALTER TABLE %s ADD COLUMN '%s'" %(db, key))

                self.c.execute('SELECT * FROM %s' %(db))
                titles = list(map(lambda x:x[0], self.c.description[1:]))
            logger.debug(f'Table "{db}" created')
            

        except sqlite3.OperationalError:
            logger.warn(f'Table "{db}" already exists')

    def update_table(self, dbs, formulas=False):
        if type(dbs) == str:
            dbs = [dbs]
        for db in dbs:
            self.delete_data(db)
            data = self.get_sheets(db, mode='data', formulas=formulas)[1:]

            with self.conn:
                self.c.execute('SELECT * FROM %s' %(db))
                titles = list(map(lambda x:x[0], self.c.description[1:]))
                
                values = '('
                for i in range(len(titles)):
                    if i == 0:
                        values = "(?"
                    else:
                        values += ", ?"

                cols = '('
                for i in range(len(titles)):
                    if i == 0:
                        cols = "('%s'"
                    else:
                        cols += ", '%s'"

                query = f'INSERT INTO %s {cols}) VALUES {values})'
                titles.insert(0, db)
                titles = tuple(titles)
                for row in range(len(data)):
                    val = tuple(data[row])
                    self.c.execute(query %titles, val)
                logger.debug(f'Table "{db}" updated')
            
    def delete_table(self, db):
        with self.conn:
            self.c.execute("DROP TABLE %s" %db)
            logger.info(f'Table "{db}" deleted')

    def delete_data(self, db):
        with self.conn:
            self.c.execute("DELETE FROM %s" %db)
            logger.debug(f'All data deleted from table "{db}"')

    def get_data(self, db, col, t_col, target):
        #Gets the value in the column by getting the row of the target column with the target value
        with self.conn:
            self.c.execute("SELECT %s FROM %s WHERE '%s' = ?" %(col, db, t_col), (target,))
            data = self.c.fetchall()
        return data

    def update_data(self, db, t_col, target, value, col):
        #Set the value in the column by getting the row of the target column with the target value
        with self.conn:
            self.c.execute("UPDATE '%s' SET '%s' = ? WHERE '%s' = ?" %(db, col, t_col), (value, target))

    def init(self, urls=None, num=False):
        if urls == None:
            urls = self.urls
        for url in urls:
            if num == url:
                self.create_table(url, True)
            else:
                self.create_table(url)
            self.update_table(url)

    def reset(self, urls=None, num=False):
        if urls == None:
            urls = self.urls
        for url in urls:
            try:
                self.delete_table(url)
            except:
                logger.warning(f"Table {url} didn't exist")
        self.init(urls, num)

    def transpose(self, a):
        mapping = map(list, zip(*a))
        transpose = []
        for i in mapping:
            transpose.append(i)
        for row in range(len(transpose)):
            for col in range(len(transpose[row])):
                if transpose[row][col] == 'FALSE' or transpose[row][col] =='False':
                    transpose[row][col] = False
                elif transpose[row][col] == 'TRUE' or transpose[row][col] =='True':
                    transpose[row][col] = True
        return transpose

    def box(self, db, data):
        sheet = self.get_sheets(db)
        values = self.get_sheets(db, mode='data')
        rows = len(values)
        validation_rule = DataValidationRule(
            BooleanCondition('BOOLEAN'), # condition'type' and 'values', defaulting to TRUE/FALSE
            showCustomUi = True)
        set_data_validation_for_cell_range(sheet, f'{data[0]}2:{data[0]}{rows}', validation_rule)
        set_data_validation_for_cell_range(sheet, f'{data[1]}2:{data[0]}{rows}', validation_rule)
        value = []
        for i in range(rows - 1):
            for j in range(values[i]):
                value.append(values[i][j])

        print(values)

    def upload_table(self, dbs, formulas=False):
        if type(dbs) == str:
            dbs = [dbs]
        for db in dbs:
            if type(db) == list:
                prior = db[1:]
                db = db[0]
            else:
                prior = []
            sheet = self.get_sheets(db, mode='work', formulas=formulas)
            remote_data = self.get_sheets(db, mode='data', formulas=formulas)

            with self.conn:
                self.c.execute('SELECT * FROM %s' %(db))
                titles = list(map(lambda x:x[0], self.c.description[1:]))
                local_data = list(map(lambda x:list(x[1:]), self.c.fetchall()))

            local_data = [titles] + local_data 
            new_data = []

            if prior != []:
                for p in range(len(prior)):
                    prior[p] = titles.index(prior[p])

            #Priority by default to the bot. If in prior list, it swithces to remote data
            
            #SETTING SAME LENGTH OF ROWS
            if len(remote_data) > len(local_data):
                for _ in range(len(remote_data) - len(local_data)):
                    local_data.append(remote_data[-1])

            elif len(remote_data) < len(local_data):
                for _ in range(len(local_data) - len(remote_data)):
                    remote_data.append(local_data[-1])

            #SETTING SAME LENGTH OF COLS. IT'S ONLY A PROBLEM IF
            #THE REMOTE HAS LESS ROWS THAN THE LOCAL
            if len(remote_data[0]) < len(local_data[0]):
                diff = len(local_data[0]) - len(remote_data[0])
                
                remote_data = self.transpose(remote_data)
                local_data = self.transpose(local_data)

                init_len = len(remote_data)

                for x in range(diff):
                    remote_data.append(local_data[init_len+x])

                remote_data = self.transpose(remote_data)
                local_data = self.transpose(local_data)
            
            for row in range(len(local_data)):
                new_data.append([])
                if local_data[row] == remote_data[row]:
                    new_data[row] = local_data[row]

                else:
                    for col in range(len(local_data[row])):
                        if col not in prior:    #BOT DEFINED
                            new_data[row].append(local_data[row][col])

                        else:   #REMOTE DEFINED
                            new_data[row].append(remote_data[row][col])
            
            while 'remove' in new_data:
                new_data.remove('remove')

            try:
                sheet.update(new_data, raw=False)
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheet.update(new_data, raw=False)
                sleep(waiting_time)
