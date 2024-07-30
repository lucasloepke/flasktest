from flask import Flask, request, render_template, redirect, url_for, send_from_directory
import os, subprocess
import scripts.expensive_statements as esmain

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/')
def index(name='User'):
    return render_template('index.html', person=name, updone=False)

@app.route('/download', methods=['POST'])
def postup(updone=True):
    if 'file1' not in request.files or 'file2' not in request.files:
        return 'No file found', 400
    file1 = request.files['file1']
    file2 = request.files['file2']
    if file1.filename == '' or file2.filename == '':
        return 'No file selected', 400
    
    file1_path = os.path.join(app.config['UPLOAD_FOLDER'], file1.filename)
    file2_path = os.path.join(app.config['UPLOAD_FOLDER'], file2.filename)
    file1.save(file1_path)
    file2.save(file2_path)

    # esmain()

    return render_template('download.html', updone=True, out=file1_path)


if __name__ == '__main__':
    app.run(debug=True)