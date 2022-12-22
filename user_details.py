from http import HTTPStatus

import shortuuid as shortuuid
from quart import Blueprint, request

from app.server import app

bp = Blueprint("reprocessed_auction", __name__)


@bp.route("/signup", methods=["POST"], endpoint="connector")
# @validate_request(ConnectorCreate)
async def sign_in(**kwargs):
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")
    user_id = shortuuid.ShortUUID().random(length=8)
    user_type = data.get("user_type")
    insert_dict = {"user_id": user_id, "password": password, "type": user_type, "email": email}
    unique_constraint_columns = ["user_id"]
    try:
        insert_result = await app.db.insert("users", insert_dict, unique_constraint_columns=unique_constraint_columns)
        if insert_result and isinstance(insert_result, int):
            return {"message": "Successfully created", "user_id": user_id}, HTTPStatus.CREATED.value
        if isinstance(insert_result, str):
            return {"message": "Failed to create credentials"}, HTTPStatus.INTERNAL_SERVER_ERROR.value
    except Exception as e:
        return {"message": f"Exception occurred {str(e)}"}, HTTPStatus.INTERNAL_SERVER_ERROR.value


@bp.route("/signin", methods=["POST"], endpoint="connector")
# @validate_request(ConnectorCreate)
async def sign_up(**kwargs):
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")
    user_type = data.get("user_type")
    try:
        user_credentials = await app.db.select(
            table="users",
            columns=["password", "user_id", "type"],
            where={"email='%s'": email},
        )
        if user_credentials[0].get("password") == password and user_credentials.get("user_type") == user_type:
            return {
                "message": "Successfully signed in",
                "user_id": user_credentials[0].get("user_id"),
            }, HTTPStatus.OK.value
        else:
            return {"message": "Invalid Credentials"}, HTTPStatus.BAD_REQUEST.value
    except Exception as e:
        return {"message": f"Exception occurred {str(e)}"}, HTTPStatus.INTERNAL_SERVER_ERROR.value
