from flask import Flask,request,render_template
import random
import os
import urllib.request
from app import app
from flask import Flask, request, redirect, jsonify
from werkzeug.utils import secure_filename


ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)

user_dict = {}
tweets_list = []


@app.route('/')
def Eval():
	return render_template('eval_data_1.html')

#def upload_file():
 #  return render_template('upload.html')


@app.route('/eval', methods=['GET', 'POST'])
def EvalData():
	print(request.form.get('data'),'+++++++++++=')
	eval_data = request.form.get("data")
	eval_data = str(eval_data)
	try:
		resp = eval(eval_data)
		return render_template('eval_data.html',answer=resp)
	except:
		return {"Message":"Error"},500


	
@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
	if request.method == 'POST':
		print(request.files)
		f = request.files['data']
		f.save(secure_filename(f.filename))
		#return render_template('upload.html')
		return render_template('upload.html')

"""""
@app.route('/file-upload', methods=['POST'])
def upload_file():
	# check if the post request has the file part
	if 'file' not in request.files:
		resp = jsonify({'message' : 'No file part in the request'})
		resp.status_code = 400
		return resp
	file = request.files['file']
	if file.filename == '':
		resp = jsonify({'message' : 'No file selected for uploading'})
		resp.status_code = 400
		return resp
	if file and allowed_file(file.filename):
		filename = secure_filename(file.filename)
		file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
		resp = eval(eval_data)
		return render_template('eval_data.html',answer=resp)
		#resp = jsonify({'message' : 'File successfully uploaded'})
		#resp.status_code = 201
		#return resp
	else:
		resp = jsonify({'message' : 'Allowed file types are txt, pdf, png, jpg, jpeg, gif'})
		resp.status_code = 400
		return resp
"""""

import os
import psutil

l1, l2, l3 = psutil.getloadavg()
CPU_use = (l3/os.cpu_count()) * 100

print("Used CPU", CPU_use)

if __name__ == "__main__":
	app.run(debug=True)




