from flask import Flask, jsonify
from whichunidb import *
from dbtools import dict_from_row

app = Flask(__name__)
wudb = WhichUniDB()

@app.route('/api/subjects')
def subjects():
    q = dict_from_row(wudb.query(WUSubject) \
            .all())
        
    return jsonify({
        "subjects": [collapse(row)for row in q]
    })

def collapse(row):
    row['alevels'] = [a['alevel'] for a in row['alevels']]
    row['careers'] = [c['career'] for c in row['careers']]
    return row

if __name__ == "__main__":
    app.run()