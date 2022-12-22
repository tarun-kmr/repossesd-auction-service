from app import settings, routes
from connections.postgres import Postgres
from quart import Quart

app = Quart(__name__)

app.config.from_object(settings)
# app = quart_cors.cors(app, allow_origin="*")


@app.before_serving
async def _init():
    # await _init_db()
    print("db initialized")

    _register_blueprints()
    print("routes registered")


async def _init_db():
    print("communication db initialized")
    comm_db_conf = app.config.get("COMM_POSTGRES")
    app.comm_db = Postgres()
    await app.comm_db.connect(
        database=comm_db_conf["NAME"],
        host=comm_db_conf["HOST"],
        port=comm_db_conf["PORT"],
        user=comm_db_conf["USER"],
        password=comm_db_conf["PASSWORD"],
        enable_read_replica=comm_db_conf["ENABLE_DB_READ_REPLICA"],
        read_replica_host=comm_db_conf["READ_REPLICA_DB_HOST"],
        read_replica_port=comm_db_conf["READ_REPLICA_DB_PORT"]
    )
    print("communication db initialized")
    return



def _register_blueprints():
    app.register_blueprint(routes.bp)
    return

