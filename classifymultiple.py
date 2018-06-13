from google.cloud import storage, language, bigquery

# Set up our GCS, NL, and BigQuery clients
storage_client = storage.Client()
nl_client = language.LanguageServiceClient()
# TODO: replace YOUR_PROJECT with your project name below
bq_client = bigquery.Client(project='elliottn-env')

dataset_ref = bq_client.dataset('classification')
dataset = bigquery.Dataset(dataset_ref)
#table_ref = dataset.table('surveymonkey')
table_ref = dataset.table('test')
table = bq_client.get_table(table_ref)

# Send article text to the NL API's classifyText method
def classify_text(article):
        response = nl_client.classify_text(
                document=language.types.Document(
                        content=article,
                        type=language.enums.Document.Type.PLAIN_TEXT
                )
        )
        return response

rows_for_bq = []
#datafile = open("surveymonkeytemp.txt", "r")
#files = datafile.read()

#files = storage_client.bucket('text-classification-codelab').list_blobs()
#files = storage_client.bucket('elliott-data-test').list_blobs()

#print files

#print("Got article files from GCS, sending them to the NL API (this will take ~2 minutes)...")

# Send files to the NL API and save the result to send to BigQuery
#for file in files:
#        if file.name.endswith('txt'):
#                article_text = file.download_as_string()

listfiles = ["test.txt", "test2.txt", "test3.txt", "test4.txt", "test5.txt", "test6.txt"]
for file in listfiles:
	datafile = open(file, "r")
	article_text = datafile.read()
	print article_text
	
	nl_response = classify_text(article_text)
	print nl_response.categories[0].name
	if len(nl_response.categories) > 0:
		rows_for_bq.append((nl_response.categories[0].name, nl_response.categories[0].confidence, article_text))

print("Writing NL API article data to BigQuery...")
# Write article text + category data to BQ
errors = bq_client.create_rows(table, rows_for_bq)
assert errors == []