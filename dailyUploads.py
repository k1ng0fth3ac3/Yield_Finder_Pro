from dbManager import Connection
from dbTables import Create_table
from dbUpload import Upload


upload = Upload()
upload.pools_history()
upload.protocols_history()
upload.protocols_chains_history()
upload.categories_history()
upload.chains_history()
upload.chains_info()
upload.protocols_info()