import functools
import json
import traceback

from google.auth.exceptions import DefaultCredentialsError

from util.get_pool import get_pool

pg_pool = None

def cloudfunction(f):
    """ Wraps a function with two arguments, the first of which is a Flask request, and the second is a
    postgresql pool, and modified it:
     - sets CORS headers and responds to OPTIONS requests to Allow-Origin *


    :param f: A function that takes a `request` and a `pgpool` and returns a json-serializable object
    :return: a function that calls f, but with CORS pre-flight handling and postgres connection pooling.
    """
    @functools.wraps(f)
    def with_cors_header(request):
        global pg_pool

        # If given an OPTIONS request, tell the requester that we allow all CORS requests (pre-flight stage)
        if request.method == 'OPTIONS':
            # Allows GET and POST requests from any origin with the Content-Type
            # header and caches preflight response for an 3600s
            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '3600'
            }

            return ('', 204, headers)

        # If it's not a CORS OPTIONS request, still include the base header.
        headers = {
            'Access-Control-Allow-Origin': '*'
        }

        # Lazily initialize a global pg_pool, so that multiple cloudfunctions can share the pool and not create one
        # each time.
        if not pg_pool:
            pg_pool = get_pool()

        try:
            function_output = f(request, pg_pool)
            return (json.dumps(function_output), 200, headers)
        except:
            traceback.print_exc()
            return (traceback.format_exc(), 500, headers)


    return with_cors_header


