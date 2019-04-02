import datetime
import time
import re
import pandas as pd
import argparse
import sys
import httplib2
from apiclient import discovery
from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
os.chdir('/home/flipped/PycharmProjects/GTM/')
def GetService():
    """Get a service that communicates to a Google API.
    Args:
      api_name: string The name of the api to connect to.
      api_version: string The api version to connect to.
      scope: A list of strings representing the auth scopes to authorize for the
        connection.
      client_secrets_path: string A path to a valid client secrets file.
    Returns:
      A service that is connected to the specified API.
    """
    client_secrets_path = 'client_secret_bigquery.json'
    api_version = 'v2'
    api_name = 'tagmanager'
    scope = ['https://www.googleapis.com/auth/tagmanager.readonly']

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=scope,
        message=tools.message_if_missing(client_secrets_path))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.

    TOKEN_FILE_NAME = 'credentials.dat'
    storage = file.Storage(TOKEN_FILE_NAME)
    credentials = storage.locked_get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())
    # Build the service object.
    service = build(api_name, api_version, http=http)
    print('service retriving......')
    return service

def Get_accountlist(service):
    accounts = service.accounts().list().execute()
    return accounts['account']

def Get_containerlist(accountlist,service):
    pathlist=[]
    containerName=[]
    for i in range(len(accountlist)):
        if accountlist[i]['name'] == 'LeasePlan Global':
            container_wrapper = service.accounts().containers().list(parent=accountlist[i]['path']).execute()
            for containerinfo in container_wrapper['container']:
                pathlist.append(containerinfo['path'])
                containerName.append(containerinfo['name'])
    return pathlist,containerName

def Get_workspacelist(containers,name,service):
    workspacelist=[]
    workspacename=[]
    containername=[]
    for i in range(len(containers)):
        workspaces = service.accounts().containers().workspaces().list(parent=containers[i]).execute()
        if name[i] == 'LP Netherlands':
            for workspace in workspaces['workspace']:
                if workspace['name'] == 'Leaseplan.com Workspace':
                    workspacelist.append(workspace['path'])
                    workspacename.append(workspace['name'])
                    containername.append(name[i])
        elif name[i] != 'LP Netherlands':
            for workspace in workspaces['workspace']:
                if workspace['name'] == 'Default Workspace':
                    workspacelist.append(workspace['path'])
                    workspacename.append(workspace['name'])
                    containername.append(name[i])
    return workspacelist,workspacename,containername

def Get_tags_triggers(workspath,workspacename,containername,service):
    tagsummary=[]
    triggersummary=[]
    final_path=[]
    final_accountId=[]
    final_containerId=[]
    final_workspaceId=[]
    final_name=[]
    final_containername=[]
    final_workspacename=[]
    for i in range(len(workspath)):
        tags = service.accounts().containers().workspaces().tags().list(parent=workspath[i]).execute()
        triggers = service.accounts().containers().workspaces().triggers().list(parent=workspath[i]).execute()
        if 'tag' in tags.keys():
            for tag in tags['tag']:
                tagsummary.append([tag,workspacename[i],containername[i]])
        if 'trigger' in triggers.keys():
            for trigger in triggers['trigger']:
                if re.match('OneTrust.*',trigger['name']) or re.match('.*Block.*Cookies',trigger['name']):
                    triggersummary.append([trigger,workspacename[i],containername[i]])

    for i in range(len(tagsummary)):
        mark = '0'
        if 'firingTriggerId' not in tagsummary[i][0].keys():
            mark = '1'
        elif 'firingTriggerId' in tagsummary[i][0].keys():
            if 'blockingTriggerId' in tagsummary[i][0].keys():
                for blockingtriggerid in tagsummary[i][0]['blockingTriggerId']:
                    for j in range(len(triggersummary)):
                        if tagsummary[i][0]['containerId'] == triggersummary[j][0]['containerId'] and tagsummary[i][0]['workspaceId'] == triggersummary[j][0]['workspaceId']:
                            if blockingtriggerid == triggersummary[j][0]['triggerId']:
                                mark = '1'
            if  tagsummary[i][0]['type'] == 'html' and  'dataLayer' in tagsummary[i][0]['parameter'][0]['value']:
                mark = '1'
        if mark == '0':
            final_path.append(tagsummary[i][0]['path'])
            final_accountId.append(tagsummary[i][0]['accountId'])
            final_containerId.append(tagsummary[i][0]['containerId'])
            final_workspaceId.append(tagsummary[i][0]['workspaceId'])
            final_name.append(tagsummary[i][0]['name'])
            final_containername.append(tagsummary[i][2])
            final_workspacename.append(tagsummary[i][1])
            # print(tagsummary[i])
    for i in triggersummary:
        if i[2] == 'LeasePlan Global':
            print(i)
    return final_path,final_accountId,final_containerId,final_workspaceId,final_name,final_containername,final_workspacename


def main():
    # try:
    accountpath= Get_accountlist(GetService())
    containerpath,containerName = Get_containerlist(accountpath,GetService())
    workspacepath,workspacename,containername = Get_workspacelist(containerpath,containerName,GetService())
    final_path,final_accountId,final_containerId,final_workspaceId,final_name,final_containername,\
    final_workspacename = Get_tags_triggers(workspacepath,workspacename,containername,GetService())
    df = {'path':final_path,'accountId':final_accountId,'containerId':final_containerId,'containerName':final_containername,
          'workspaceId':final_workspaceId,'workspaceName':final_workspacename,
          'tagName':final_name}
    df = pd.DataFrame(df)

    df.to_excel('tags.xlsx',index=False)
    bigquery_key = 'bigquery_key.json'
    pd.io.gbq.to_gbq(df, 'gtmtagot.tagswithoutottrigger', 'leaseplan-1515678200989',
                     if_exists='replace',
                     private_key=bigquery_key)
    # except:
    #     local_data = pd.read_excel('tags.xlsx')
    #     bigquery_key = 'bigquery_key.json'
    #     pd.io.gbq.to_gbq(local_data, 'gtmtagot.tagswithoutottrigger', 'leaseplan-1515678200989',
    #                      if_exists='replace',
    #                      private_key=bigquery_key)

if __name__ == '__main__':
    main()