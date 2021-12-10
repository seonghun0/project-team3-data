from flask import Flask
from api.team_one import team_one
from api.team_two import team_two
from api.team_three import team_three

app = Flask(__name__)
app.register_blueprint(team_one, url_prefix='/team-one')
app.register_blueprint(team_two, url_prefix='/team-two')
app.register_blueprint(team_three, url_prefix='/team-three')

@app.route("/")
def index():
    return "This is API Service App"

