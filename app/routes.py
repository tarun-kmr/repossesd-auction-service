from http import HTTPStatus
import json
from datetime import datetime
from quart import Blueprint
from app.settings import BASE_ROUTE
from quart import request, current_app as app, make_response, g


bp = Blueprint("communication", __name__, url_prefix=BASE_ROUTE)

@bp.route('/user')
async def index():
    return '<h1>I got worms</h1>'


@bp.route('/up')
async def up():
    return 'Up'