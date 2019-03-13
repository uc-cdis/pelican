from api import create_app
from spark.spark import init_spark_context


def run_server(app):
    app.run(host='0.0.0.0')


if __name__ == '__main__':
    sc = init_spark_context()
    app = create_app('development', sc)
    run_server(app)
