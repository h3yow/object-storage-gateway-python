"""Local object storage gateway entrypoint."""
from app.api import create_app
from app.config import load_settings

app = create_app(load_settings())
