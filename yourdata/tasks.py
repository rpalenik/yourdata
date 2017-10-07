from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@52.54.155.107//')


@app.task
def add(x, y):
    return x + y
