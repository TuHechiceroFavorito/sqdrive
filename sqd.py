import sqlite3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import numpy as np
from time import asctime, localtime, sleep
from gspread_formatting import BooleanCondition, DataValidationRule, set_data_validation_for_cell_range
import schedule

waiting_time = 2
wait_exceed = 20

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG
)

logger = logging.getLogger(__name__)

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]


urls = {'data': 'your_drive_sheet_url'}

creds = ServiceAccountCredentials.from_json_keyfile_name('creds/cred.json', scope)

client = gspread.authorize(creds)

class DBuilder:
    def __init__(self, urls, dbname=':memory:'):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname, check_same_thread=False)
        self.c = self.conn.cursor()
        self.urls = urls

    def get_sheets(self, db, mode=False, sheet=None, tab=None):
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
                sheets = client.open_by_url(urls[db])
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheets = client.open_by_url(urls[db])
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
                    data = sheet.get_all_values()
                    sleep(waiting_time)
                except:
                    logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                    sleep(wait_exceed)
                    data = sheet.get_all_values()
                    sleep(waiting_time)

                logger.debug(f'Values retrieved "{db}"')
                
                return data
        
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
                    self.c.execute('ALTER TABLE %s ADD COLUMN "%s"' %(db, key))

                self.c.execute('SELECT * FROM %s' %(db))
                titles = list(map(lambda x:x[0], self.c.description[1:]))
            logger.debug(f'Table "{db}" created')
            

        except sqlite3.OperationalError:
            logger.warn(f'Table "{db}" already exists')

    def update_table(self, dbs):
        if type(dbs) == str:
            dbs = [dbs]
        for db in dbs:
            self.delete_data(db)
            data = self.get_sheets(db, mode='data')[1:]

            with self.conn:
                self.c.execute('SELECT * FROM %s' %(db))
                titles = list(map(lambda x:x[0], self.c.description[1:]))
                
                values = '('
                for i in range(len(titles)):
                    if i == 0:
                        values = '(?'
                    else:
                        values += ', ?'

                cols = '('
                for i in range(len(titles)):
                    if i == 0:
                        cols = '("%s"'
                    else:
                        cols += ', "%s"'

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

    def init(self, urls=urls, num=False):
        for url in urls:
            if num == url:
                dh.create_table(url, True)
            else:
                dh.create_table(url)
            dh.update_table(url)

    def reset(self, urls=urls, num=False):
        for url in urls:
            try:
                self.delete_table(url)
            except:
                logger.warning(f"Table {url} didn't exist")
        self.init(urls, num)

    def transpose(self, name):
        numpy_array = np.array(name)
        transpose = numpy_array.T
        name = transpose.tolist()
        for row in range(len(name)):
            for col in range(len(name[row])):
                if name[row][col] == 'FALSE' or name[row][col] =='False':
                    name[row][col] = False
                elif name[row][col] == 'TRUE' or name[row][col] =='True':
                    name[row][col] = True
        return name

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

    def upload_table(self, dbs):
        if type(dbs) == str:
            dbs = [dbs]
        for db in dbs:
            if type(db) == list:
                cols = db[1:]
                db = db[0]
            else:
                cols = []

            sheet = self.get_sheets(db, mode='work')
            old_data = self.get_sheets(db, mode='data')[1:]

            with self.conn:
                self.c.execute('SELECT * FROM %s' %(db))
                titles = list(map(lambda x:x[0], self.c.description[1:]))
                data = list(map(lambda x:list(x[1:]), self.c.fetchall()))

            data = self.transpose(data)
            old_data = self.transpose(old_data)

            new_data = []
            for element in data:
                col = titles[data.index(element)]
                if col in cols:
                    element = old_data[data.index(element)]

                new_data.append(element)

            for title in titles:
                if title[:5] == 'empty':
                    titles[titles.index(title)] = ''

            new_data = self.transpose(new_data)

            new_data.insert(0, titles)

            try:
                sheet.update(new_data)
                sleep(waiting_time)
            except:
                logger.error(f'Request limit exceeded. Waiting for {wait_exceed} seconds...')
                sleep(wait_exceed)
                sheet.update(new_data)
                sleep(waiting_time)



if __name__ == '__main__':
    dh = DBuilder('example.db')
    dh.init(urls)
