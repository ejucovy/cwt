from main.forms import BatchForm, get_task_log
from django import forms
from actionkit import Client
from actionkit.rest import client as RestClient
from actionkit.models import CoreAction, CoreActionField
from django.db import models
import traceback
from django.conf import settings

class WelcomeForm(BatchForm):
    help_text = """
The SQL must return a column named `user_id`.

All columns with prefix `new_data_` will be treated as new values 
for the core_user attributes.  For example, `select user_id, "United Kingdom" 
as new_data_country, "London" as new_data_city, country, city from core_user 
where id in (100,101,102,103);` would cause four user records to have their 
country and city attributes set to "United Kingdom" and "London" respectively.

Columns prefixed new_data_user_* can also be used to set or update userfield
values.

All columns apart from user_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""

    def run(self, task, rows):
        ak = Client()
        
        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("user_id") and int(row['user_id'])
            assert row.get("employer")
            assert row.get("num_employees") and int(row['num_employees'])
            assert row.get("threshold") and int(row['threshold'])
            assert row.get("processing_page_name") and int(row['processing_page_name'])
            assert row.get("originating_page_id") and int(row['originating_page_id'])
            assert row.get("originating_action_id") and int(row['originating_action_id'])

            try:
                f = CoreActionField.objects.using("ak").select_related("action").filter(
                    action__page__name=row['originating_page_name'],
                    action__user__id=int(row['user_id']),
                    name="welcome_employer",
                    value=row['employer'])[0]
            except IndexError:
                pass
            else:
                task_log.activity_log(task, {"id": row['user_id'],
                                             "existing_action": f.action.id})
                continue
            
            data = {
                'id': row['user_id'],
                'page': row['processing_page_name'],
                'action_welcome_employer': row['employer'],
                'action_welcome_num_employees': row['num_employees'],
                'user_welcome_employer': row['employer'],
                'user_welcome_num_employees': row['num_employees'],                
            }
            data['action_originating_page_url'] = data['user_originating_page_url'] = "@@TODO"
            
            task_log.activity_log(task, data)
            try:
                resp = ak.act(data)
                task_log.success_log(task, resp)
            except Exception, e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['user_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

from main.task_registry import register_task
register_task("WelcomeJob", "Welcome Prep", WelcomeForm)