import pandas as pd
class Tables:
    def __init__(self,columns_table,table_name):
        self.columns = columns_table
        self.data = pd.DataFrame(columns=self.columns)
        self.table_name = table_name
        pass