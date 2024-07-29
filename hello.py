from flask import Flask, render_template, url_for

app = Flask(__name__)

@app.route('/')
def index(name='Guest'):
    return render_template('index.html', person=name)
