from main.forms import BatchForm, get_task_log
from django import forms
from actionkit import Client
from actionkit.rest import client as RestClient
from actionkit.models import CoreAction, CoreActionField
from django.db import models
import traceback
from django.conf import settings

class UserModificationForm(BatchForm):
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
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("user_id") and int(row['user_id'])

            new_values = {"id": row['user_id']}
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                new_values[key.replace("new_data_", "", 1)] = row[key]

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                rest.user.put(id=row['user_id'], **new_values)
                resp = {}
                resp['log_id'] = row['user_id']
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
register_task("UserModificationJob", "Modify Users", UserModificationForm)
