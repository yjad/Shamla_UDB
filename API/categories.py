#! getcategories-API using flask
from flask import Flask, jsonify, abort, make_response, request
from types import *
from DBhandling import *
import logging

logging.basicConfig(level=logging.DEBUG)


SERVER_URL = 'booksapi.islam-db.com'
app = Flask(__name__)

def validate_request_pagination(req):
    limit = 10
    offset = 0

    if 'limit' in req.json and type(req.json['limit']) is not int:
        logging.debug ('limit is not int')
        return -1

    if 'offset' in req.json and type(req.json['offset']) is not int:
        logging.debug ('offset is not int')
        return -1

    if 'limit' in request.json:
        limit = request.json['limit']

    if 'offset' in request.json:
        offset = request.json['offset']

    return 0, limit, offset

def validate_books_request(req):
    keywords = ""
    of_text = ""
    categ_id = -1

    if 'keywords' in req.json and type(req.json['keywords']) != str:
        logging.debug('keywords is not string')
        return -1

    if 'of' in req.json and type(req.json['of']) != str:
        logging.debug('"of" is not string')
        return -1

    if 'id' in req.json and type(req.json['id']) is not int:
        logging.debug ('id is not int')
        return -1

    if 'keywords' in request.json:
        keywords = request.json['keywords']

    if 'of' in request.json:
        of_text = request.json['of']

    if of_text in ('category', 'author'):
        if 'id' in request.json:
            categ_id = request.json['id']

    status, limit, offset = validate_request_pagination(req)
    if status == -1:
        return status

    return 0, keywords, of_text, categ_id, limit, offset

def validate_authors_request(req):
    keywords = ""

    if 'keywords' in req.json and type(req.json['keywords']) != str:
        logging.debug('keywords is not string')
        return -1

    if 'keywords' in request.json:
        keywords = request.json['keywords']

    status, limit, offset = validate_request_pagination(req)
    if status == -1:
        return status

    return 0, keywords, limit, offset

def validate_categories_request(req):
    return validate_request_pagination(req)


@app.route('/api/v1/categories/<int:parentid>/more/<int:id>', methods = ['POST'])
def get_categories(parentid, id):
    status, keywords, limit, offset = validate_books_request(request)

    if status != 0:
        abort(400, status)

    sql = f"SELECT id, name FROM cat WHERE catord = {parentid} LIMIT {limit}"
    #cmd = f"SELECT id, name FROM cat WHERE catord = {parentid}"
    logging.debug(sql)
    categs = categories_db(sql)
    return jsonify({'Getgories': categs})

@app.route('/api/v1/books/<int:id>', methods = ['POST'])
def books(id):

    status, keywords, of_text, categ_id, limit, offset = validate_books_request(request)
    if status != 0:
        abort(400, status)
    logging.debug (f"of: {of_text}")

# case 1: id =0, return all book details
    if id != 0:
        sql = f"SELECT * FROM bok WHERE bkid = {id}"
        logging.debug(sql)
        book_list = books_db(sql)
        return jsonify({'books': book_list})
    elif of_text == "":
        sql = f"SELECT bkid, bk FROM bok WHERE bk like '%{keywords}%' ORDER BY bkid LIMIT {limit} OFFSET {offset}"
        logging.debug(sql)
        book_list = books_db(sql)
        return jsonify({'books': book_list})
    elif of_text == "category":
        sql = f"SELECT bkid, bk FROM bok WHERE cat = {categ_id} and bk like '%{keywords}%' ORDER BY bkid LIMIT {limit} OFFSET {offset}"
        logging.debug(sql)
        book_list = books_db(sql)
        return jsonify({'books': book_list})
    elif of_text == "author":
        sql = f"SELECT bkid, bk FROM bok WHERE authno = {categ_id} and bk like '%{keywords}%' ORDER BY bkid LIMIT {limit} OFFSET {offset}"
        logging.debug(sql)
        book_list = books_db(sql)
        return jsonify({'books': book_list})
    # case 2: no id is provided, return book short details with proper pagination

@app.route('/api/v1/authors/<int:id>', methods = ['POST'])
def authors(id):
    status, keywords,  limit, offset = validate_authors_request(request)
    if status != 0:
        abort(400, status)

# case 1: id =0, return all author's details
    if id != 0:
        sql = f"SELECT * FROM auth WHERE authid = {id}"
        logging.debug(sql)
        auth_list = books_db(sql)
        return jsonify({'authors': auth_list})
    else: # case 2: no id is provided, return author short details with proper pagination
        sql = f"SELECT authid, auth FROM auth WHERE auth like '%{keywords}%' ORDER BY authid LIMIT {limit} OFFSET {offset}"
        logging.debug(sql)
        auth_list = books_db(sql)
        return jsonify({'authors': auth_list})

if __name__ == '__main__':
    app.run(debug = True)



