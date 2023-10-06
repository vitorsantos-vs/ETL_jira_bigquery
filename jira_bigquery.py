import jira
import pandas as pd
from jira import JIRA
import pandas_gbq
from google.oauth2 import service_account

# Autenticação no Jira
jira = JIRA(options={'server': 'https://vitorsantos2301.atlassian.net'},
            basic_auth=("vitor.aparecidosantos18@gmail.com",
                        "ATATT3xFfGF0UxO84ZOYLAgf4Vrb-lpqUlBxXbhggghRGWpMz_cjfz855JPq1Q5eTz-QJ_rv9l4NAdaz9OMbwCUhyr5HHDZkiIEpyfS8eKJz53Q3NAH3csVaP1-4P9MEweG8y6E0YGDcy-4KlKWL7B44wSAFNE8v3SoxesHykeU5MXwgygKePFo=FBDD383F"))

# Lista de chaves das issues que você deseja carregar
issue_keys = ['JIRACLOUD-1', 'JIRACLOUD-2', 'JIRACLOUD-3', 'JIRACLOUD-4', 'JIRACLOUD-5']

# Cria um DataFrame vazio
df = pd.DataFrame()

# Itera sobre as chaves das issues e adiciona cada uma como uma nova linha no DataFrame
for key in issue_keys:
    singleIssue = jira.issue(key)
    row = {
        'key': singleIssue.key,
        'summary': singleIssue.fields.summary,
        'assignee': singleIssue.fields.assignee.displayName,
        'reporter': singleIssue.fields.reporter.displayName,
        'project': singleIssue.fields.project.key, 
        'description': singleIssue.fields.description,
        'status': singleIssue.fields.status.name,
        'created': singleIssue.fields.created,
        'updated': singleIssue.fields.updated,
    }
    df = df.append(row, ignore_index=True)

# Converte as colunas de data para o tipo datetime
df['created'] = pd.to_datetime(df["created"])
df['updated'] = pd.to_datetime(df["updated"])

# Função para salvar os dados no BigQuery
def save_gbq():
    try:
        SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials = service_account.Credentials.from_service_account_info(
            {
            "type": "service_account",
            "project_id": "jira-cloud-bq",
            "private_key_id": "27e8edddc5b51dc49542d89f6a24ac4fbeb834ed",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCpXYwsQlocISkK\nRNrOC7WRYeZATaKSG/iVdQhlVLhj7VS7NJvr7Sl/6AR++L48UHzn38y2UKK4PRDp\nRZUhPnih+h4DisFKdfA5L/34c1/25cIFv5w7Fbz8rpR5PROm+U3v2rOPiuJv3EfN\n55Dxyf8K1irQC9Uvrj3GyVvp1N5iZJ2eEyOnMrNFJEET2nmg+EE/Mpv/dWLJUwck\nTk4cUAEJRY0adECVDZhsMmcy6WT0vAEbeTVQceoFtt8BS27UYjZnetrLIbXu6/T0\nKNw7ROC8vRuF0cLk0wy6hFP+O/YYyZER4+w19Xg71Ap7D/qWJX12pzStCBnx5kqP\ncmB75nFZAgMBAAECggEAAZWoSXtnfVAi4psneT6pAXgoMIKrn2OWflSq7db11+sM\nDDU1xGOnFMf+3fR7fitbwFxn4GLZaUKUu4EXJfUOqYp8hxtu7oN+8wLvYdcamJ/3\nE7M26i6mlAtq4a7q9buT4Li+2ZZ5WiMZN3pXHKjt98D/1R0XxgqllbTa4jGEv6K/\nbGzva0fLBqBckHbDH4yfEUI/rpj0ONbPf8umBItsCHnh+ptd7N/DeMabVh5jOUNv\nO52R6Y3eib32pchnxYFgP3AIg455dRjIqlhCsvEqcQQrPuaRJcX8ScMXf8NtM2L3\nFEZ8VgKKX53tb7MfCkHgXcjE/4VFMnZUEluv+GCacQKBgQDj+ugKpRD5cyYsl6mI\nsiPQBbW/VruMjkb4i7Kq0fx7h4MM+DRf4vRrieAPfl3t18ZFRBsx6HhLvRTLATBe\nN674QK66DgnF19AvgRr3Dk1J5CMlC0kNm2jlQ/OS8CgCciEUI7JkX4uPyobVQaPs\n6UJwyOhoccQXerMmOSuqLRuyaQKBgQC+LmhIsGcLsGbvjo2UrCe1flam6bbN/Z9q\n2mD4q2gL2i6Ybdzg2LbgDoU2SDf0aiLxs6/LTf3IhNG4LhDXhJvGo4Nf6PglGi6F\nRDYJre/XyVv0OEIBvfmdyvXjrZD8JAeB/LUyU80jG2mV14lW+8IuZ9LJUhobWi1u\nCJ30008JcQKBgQDVQGMDagopvEtuFOOxiSi0slKiy0eeH4xFe69B2DB7gHPWkumy\n88G8QfkDpSnJjDfbnOxvT8wO1Kx5hAAxcZbKgv6t7Om5VblYZkTJ6mfjgXJaeWb5\nQnDpXBmgTeKu/wDwLL/24eTTORfMrZzfxAWxKE01PY22hMDFNV8hzRYGWQKBgQCR\nnq0hA1lTnPWkdi24af1ZvewRkCD2Jz3arm3T2kMn2B8Xff/DdozIkLW1c6TMlDw/\ngAuaCxTIZdLDSRqCOzqf5XHhjVv75Mk8J5eM388nFjo/jJgWB2XLSa3vcGX0eo35\nHQuNBperSwEUx92Uad8sczj2B9SPnwPsHdL8tJPvMQKBgQCES16wmBDTFDGL+5RR\ni3e8hkMy5yU66vNr5/fMGUGJ676WmagG6bs8mI4VMdg4P88+g/+927dcfr5IYEm8\nEovkvGO41kgQw+03blP1KD1xKE1d+wrPP/UjOFusFU+mV79vLd0swVMGHdMrZOMm\njzaFHwGSlG/LysY/QszRJwIqDQ==\n-----END PRIVATE KEY-----\n",
            "client_email": "bq-jira@jira-cloud-bq.iam.gserviceaccount.com",
            "client_id": "109349687851196604449",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bq-jira%40jira-cloud-bq.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
            }
        )
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = "jira-cloud-bq"
        pandas_gbq.to_gbq(df, 'dados_bq.jira-bq', project_id='jira-cloud-bq', if_exists='replace')
        print("Dados carregados com sucesso no BigQuery")
    except Exception as e:
        print("Dados não carregados no BigQuery", e)

save_gbq()
print(df)
