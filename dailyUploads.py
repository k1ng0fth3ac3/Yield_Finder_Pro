from dbManager import Connection
from dbUpload import Upload

upload = Upload()
#upload.pools_info()
#upload.pools_history(overWriteTheDay=True)
#upload.protocols_history(overWriteTheDay=True)
#upload.protocols_chains_history(overWriteTheDay=True)
#upload.categories_history(overWriteTheDay=True)
#upload.chains_history(overWriteTheDay=True)
#upload.chains_info()
#upload.protocols_info()
#upload.token_contracts()
upload.result_table(min_apy=500,topNpools=100)            # There's an issue with the TOP pool parameter