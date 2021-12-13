from flask import Blueprint
from flask import jsonify
from flask import make_response
import json

team_one = Blueprint('team_one', __name__)

@team_one.route("/")
def index():
    return "This is Api Service for Team One"

@team_one.route("/demo-one")
def demo_one() :
    return {
        "name": "John Doe",
        "email": "johndoe@example.com",
        "phone": "010-9438-4907",
        "birth": "1990-07-23"
    }

@team_one.route("/demo-two")
def demo_two() :
    return jsonify([{
        "name": "John Doe",
        "email": "johndoe@example.com",
        "phone": "010-9438-4907",
        "birth": "1990-07-23"
    }, {
        "name": "홍길동",
        "email": "hkd@example.com",
        "phone": "010-8687-2399",
        "birth": "1990-04-21"
    }])

@team_one.route("/demo-three")
def demo_three() :
    response = make_response(json.dumps([{
                "name": "John Doe",
                "email": "johndoe@example.com",
                "phone": "010-9438-4907",
                "birth": "1990-07-23"
            }, {
                "name": "홍길동",
                "email": "hkd@example.com",
                "phone": "010-8687-2399",
                "birth": "1990-04-21"
            }], ensure_ascii=False, indent=4))
    response.content_type = "application/json;charset=utf-8"

    return response