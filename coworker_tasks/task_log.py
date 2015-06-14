from .models import LogEntry
import json
import datetime

class TaskLogger(object):

    def _log(self, task, type, *args):
        dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.time) else None
        log_str = json.dumps(args, default=dthandler)
        LogEntry(task=task, type=type, data=log_str).save()
        
    def activity_log(self, task, *args):
        self._log(task, "activity", *args)
            
    def sql_log(self, task, *args):
        self._log(task, "sql", *args)

    def error_log(self, task, *args):
        self._log(task, "error", *args)

    def success_log(self, task, *args):
        self._log(task, "success", *args)
        
