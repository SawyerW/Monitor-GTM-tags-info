# Monitor-GTM-tags-info
Check GTM container tags and triggers with Google service API
The get-tag-list.py script contains script about how to connect google service api, and how to retrieve gtm tag info. data from google service api is very structured and in json format, you can very easily find info you are looking for based on the structure.
get-tag-list.py script also contains script how to update the GTM tag info to bigquery through pandas and google service account.
airflow_schedule.py contains a very easy script how to automatically run the get-tag-list.py each day.
