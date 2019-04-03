runtime: python37
entrypoint: gunicorn -b :$PORT jobdata_server:app --pythonpath ./src
handlers:
  # This configures Google App Engine to serve the files in the app's static
  # directory.
- url: /static
  static_dir: static

  # This handler routes all requests not caught above to your main app. It is
  # required when static routes are defined, but can be omitted (along with
  # the entire handlers section) when there are no static files defined.
- url: /.*
  script: auto
