import sys
module = sys.modules[__name__]
config = {'ui.bind_host': '0.0.0.0', 'ui.port': 8088, 'ui.timeout': 60, 'ui.workers': 4, 'SQLALCHEMY_DATABASE_URI': 'sqlite:////home/evan/workspace/awsdx-hackathon/.meltano/utilities/superset/superset.db', 'SECRET_KEY': 'wi23f/r7bpTUtM7wZJYclTCEOdF0ZjzRba3CNHCzW8a02NZspWjhWq7R'}
for key, value in config.items():
    if key.isupper():
        setattr(module, key, value)