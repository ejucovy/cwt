from django.conf.urls.defaults import patterns, include, url

urlpatterns = patterns(
    '',
    url(r'task-review/$', 'coworker_tasks.views.task_review'),
    )
