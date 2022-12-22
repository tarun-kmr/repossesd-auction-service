from http import HTTPStatus
import json
from datetime import datetime
from quart import Blueprint
from ..settings import BASE_ROUTE 
from quart import request, current_app as app, make_response, g


bp = Blueprint("communication", __name__, url_prefix=BASE_ROUTE)

@app.route('/')
async def index():
    return '<h1>I got worms</h1>'


@app.route('/up')
async def up():
    return 'Up'