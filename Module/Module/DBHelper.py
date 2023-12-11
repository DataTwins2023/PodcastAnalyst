import pymysql

class DBHelper:
    """使用pymysql模組化建立db連線

    Return:
        回傳已建立連線之物件
    """
    def __init__(self, host, username, password, db_name, charset):
        """
        初始化DB連線
        """
        self.host = host
        self.username = username
        self.password = password
        self.db_name = db_name
        self.charset = charset
        self.connection = self._connection()
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)

    def _connection(self):
        """
        建立DB連線
        """
        return pymysql.connect(host=self.host, 
                                user=self.username, 
                                password=self.password, 
                                db=self.db_name, 
                                charset=self.charset)