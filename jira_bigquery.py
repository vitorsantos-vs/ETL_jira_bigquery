import jira
import pandas as pd
from jira import JIRA
import pandas_gbq
from google.oauth2 import service_account

# Autenticação no Jira
jira = JIRA(options={'server': 'serve jira'},
            basic_auth=("email da conta jira",
                        "token jira"))

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
            "credências bigquery"
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
