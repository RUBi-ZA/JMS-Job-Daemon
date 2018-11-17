import os

EMAIL_USE_TLS = True
EMAIL_HOST = 'smtp.live.com'
EMAIL_PORT = 587
EMAIL_HOST_USER = os.environ.get("EMAIL_HOST_USER", 'jms.rubi@outlook.com')
EMAIL_HOST_PASSWORD = os.environ.get("EMAIL_HOST_PASSWORD", 'JMS>Galaxy')
DEFAULT_FROM_EMAIL = EMAIL_HOST_USER
