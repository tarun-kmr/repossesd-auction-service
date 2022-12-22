from app import settings, routes
from connections.postgres import Postgres
from quart import Quart

app = Quart(__name__)

app.config.from_object(settings)


@app.before_serving
async def _init():
    await _init_db()
    app.logger.info("db initialized")

    _register_blueprints()
    app.logger.info("routes registered")


async def _init_db():
    db_conf = app.config.get("POSTGRES")
    app.db = Postgres(
        database=db_conf["NAME"],
        host=db_conf["HOST"],
        port=db_conf["PORT"],
        user=db_conf["USER"],
        password=db_conf["PASSWORD"],
        enable_read_replica=db_conf["ENABLE_DB_READ_REPLICA"],
        read_replica_host=db_conf["READ_REPLICA_DB_HOST"],
        read_replica_port=db_conf["READ_REPLICA_DB_PORT"],
        publish_dashboard_events=app.config.get("PUBLISH_DASHBOARD_EVENTS", False),
        dashboard_producer=app.dashboardProducer,
    )
    await app.db.connect()
    app.logger.info("recovery db initialized")
    comm_db_conf = app.config.get("COMM_POSTGRES")
    app.comm_db = Postgres(
        database=comm_db_conf["NAME"],
        host=comm_db_conf["HOST"],
        port=comm_db_conf["PORT"],
        user=comm_db_conf["USER"],
        password=comm_db_conf["PASSWORD"],
        enable_read_replica=comm_db_conf["ENABLE_DB_READ_REPLICA"],
        read_replica_host=comm_db_conf["READ_REPLICA_DB_HOST"],
        read_replica_port=comm_db_conf["READ_REPLICA_DB_PORT"],
    )
    await app.comm_db.connect()
    app.logger.info("communication db initialized")
    return



def _register_blueprints():
    app.register_blueprint(routes.bp)
    return

